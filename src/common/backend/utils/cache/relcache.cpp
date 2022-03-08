/* -------------------------------------------------------------------------
 *
 * relcache.c
 *	  openGauss relation descriptor cache code
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/relcache.c
 *
 * -------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		RelationCacheInitialize			- initialize relcache (to empty)
 *		RelationCacheInitializePhase2	- initialize shared-catalog entries
 *		RelationCacheInitializePhase3	- finish initializing relcache
 *		RelationIdGetRelation			- get a reldesc by relation id
 *		RelationClose					- close an open relation
 *
 * NOTES
 *		The following code contains many undocumented hacks.  Please be
 *		careful....
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/file.h>
#include <catalog/pg_obsscaninfo.h>

#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/multixact.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/catversion.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_auth_history.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_directory.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_index.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/gs_job_argument.h"
#include "catalog/gs_job_attribute.h"
#include "catalog/gs_asp.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_largeobject_metadata.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_range.h"
#include "catalog/pg_recyclebin.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_uid.h"
#include "catalog/pg_uid_fn.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_user_status.h"
#include "catalog/pg_workload_group.h"
#include "catalog/gs_policy_label.h"
#include "catalog/gs_auditing_policy.h"
#include "catalog/gs_auditing_policy_acc.h"
#include "catalog/gs_auditing_policy_filter.h"
#include "catalog/gs_auditing_policy_priv.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/gs_masking_policy_actions.h"
#include "catalog/gs_masking_policy_filters.h"

#include "catalog/gs_encrypted_proc.h"
#include "catalog/gs_encrypted_columns.h"
#include "catalog/gs_column_keys.h"
#include "catalog/gs_column_keys_args.h"
#include "catalog/gs_client_global_keys.h"
#include "catalog/gs_client_global_keys_args.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/gs_matview.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/pg_snapshot.h"
#include "catalog/gs_opt_model.h"
#include "catalog/gs_global_chain.h"
#ifdef PGXC
#include "catalog/pgxc_class.h"
#include "catalog/gs_global_config.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_slice.h"
#endif
#include "catalog/schemapg.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_streaming_stream.h"
#include "catalog/pg_streaming_cont_query.h"
#include "catalog/pg_streaming_reaper_status.h"
#include "catalog/gs_model.h"
#include "commands/matview.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "pgstat.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/locator.h"
#include "postmaster/autovacuum.h"
#include "replication/catchup.h"
#include "replication/walsender.h"
#endif
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "storage/lmgr.h"
#include "storage/page_compression.h"
#include "storage/smgr/smgr.h"
#include "storage/smgr/segment.h"
#include "threadpool/threadpool.h"
#include "storage/tcap.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/resowner.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/resowner.h"
#include "utils/evp_cipher.h"
#include "access/csnlog.h"
#include "access/cstore_am.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "replication/walreceiver.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/common/ts_tablecmds.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "utils/knl_relcache.h"
#include "utils/knl_partcache.h"
#include "utils/knl_localtabdefcache.h"

/*
 *		name of relcache init file(s), used to speed up backend startup
 */
#define RELCACHE_INIT_FILENAME "pg_internal.init"

#define RELCACHE_INIT_FILEMAGIC 0x573266 /* version ID value */

/*
 *		hardcoded tuple descriptors, contents generated by genbki.pl
 */
extern const FormData_pg_attribute Desc_pg_class[Natts_pg_class] = {Schema_pg_class};
extern const FormData_pg_attribute Desc_pg_attribute[Natts_pg_attribute] = {Schema_pg_attribute};
extern const FormData_pg_attribute Desc_pg_proc[Natts_pg_proc] = {Schema_pg_proc};
extern const FormData_pg_attribute Desc_pg_type[Natts_pg_type] = {Schema_pg_type};
extern const FormData_pg_attribute Desc_pg_database[Natts_pg_database] = {Schema_pg_database};
extern const FormData_pg_attribute Desc_pg_authid[Natts_pg_authid] = {Schema_pg_authid};
extern const FormData_pg_attribute Desc_pg_auth_members[Natts_pg_auth_members] = {Schema_pg_auth_members};
extern const FormData_pg_attribute Desc_pg_index[Natts_pg_index] = {Schema_pg_index};
extern const FormData_pg_attribute Desc_pg_user_status[Natts_pg_user_status] = {Schema_pg_user_status};

static const FormData_pg_attribute Desc_pg_default_acl[Natts_pg_default_acl] = {Schema_pg_default_acl};
static const FormData_pg_attribute Desc_pg_pltemplate[Natts_pg_pltemplate] = {Schema_pg_pltemplate};
static const FormData_pg_attribute Desc_pg_tablespace[Natts_pg_tablespace] = {Schema_pg_tablespace};
static const FormData_pg_attribute Desc_pg_shdepend[Natts_pg_shdepend] = {Schema_pg_shdepend};
static const FormData_pg_attribute Desc_pg_foreign_server[Natts_pg_foreign_server] = {Schema_pg_foreign_server};
static const FormData_pg_attribute Desc_pg_user_mapping[Natts_pg_user_mapping] = {Schema_pg_user_mapping};
static const FormData_pg_attribute Desc_pg_foreign_data_wrapper[Natts_pg_foreign_data_wrapper] = {
    Schema_pg_foreign_data_wrapper};
static const FormData_pg_attribute Desc_pg_shdescription[Natts_pg_shdescription] = {Schema_pg_shdescription};
static const FormData_pg_attribute Desc_pg_aggregate[Natts_pg_aggregate] = {Schema_pg_aggregate};
static const FormData_pg_attribute Desc_pg_am[Natts_pg_am] = {Schema_pg_am};
static const FormData_pg_attribute Desc_pg_amop[Natts_pg_amop] = {Schema_pg_amop};
static const FormData_pg_attribute Desc_pg_amproc[Natts_pg_amproc] = {Schema_pg_amproc};
static const FormData_pg_attribute Desc_pg_attrdef[Natts_pg_attrdef] = {Schema_pg_attrdef};
static const FormData_pg_attribute Desc_pg_cast[Natts_pg_cast] = {Schema_pg_cast};
static const FormData_pg_attribute Desc_pg_constraint[Natts_pg_constraint] = {Schema_pg_constraint};
static const FormData_pg_attribute Desc_pg_conversion[Natts_pg_conversion] = {Schema_pg_conversion};
static const FormData_pg_attribute Desc_pg_depend[Natts_pg_depend] = {Schema_pg_depend};
static const FormData_pg_attribute Desc_pg_description[Natts_pg_description] = {Schema_pg_description};
static const FormData_pg_attribute Desc_pg_inherits[Natts_pg_inherits] = {Schema_pg_inherits};
static const FormData_pg_attribute Desc_pg_language[Natts_pg_language] = {Schema_pg_language};
static const FormData_pg_attribute Desc_pg_largeobject[Natts_pg_largeobject] = {Schema_pg_largeobject};
static const FormData_pg_attribute Desc_pg_namespace[Natts_pg_namespace] = {Schema_pg_namespace};
static const FormData_pg_attribute Desc_pg_opclass[Natts_pg_opclass] = {Schema_pg_opclass};
static const FormData_pg_attribute Desc_pg_operator[Natts_pg_operator] = {Schema_pg_operator};
static const FormData_pg_attribute Desc_pg_rewrite[Natts_pg_rewrite] = {Schema_pg_rewrite};
static const FormData_pg_attribute Desc_pg_statistic[Natts_pg_statistic] = {Schema_pg_statistic};
static const FormData_pg_attribute Desc_pg_trigger[Natts_pg_trigger] = {Schema_pg_trigger};
static const FormData_pg_attribute Desc_pg_opfamily[Natts_pg_opfamily] = {Schema_pg_opfamily};
static const FormData_pg_attribute Desc_pg_db_role_setting[Natts_pg_db_role_setting] = {Schema_pg_db_role_setting};
static const FormData_pg_attribute Desc_pg_largeobject_metadata[Natts_pg_largeobject_metadata] = {
    Schema_pg_largeobject_metadata};
static const FormData_pg_attribute Desc_pg_extension[Natts_pg_extension] = {Schema_pg_extension};
static const FormData_pg_attribute Desc_pg_foreign_table[Natts_pg_foreign_table] = {Schema_pg_foreign_table};
static const FormData_pg_attribute Desc_pg_statistic_ext[Natts_pg_statistic_ext] = {Schema_pg_statistic_ext};
static const FormData_pg_attribute Desc_pg_rlspolicy[Natts_pg_rlspolicy] = {Schema_pg_rlspolicy};
static const FormData_pg_attribute Desc_pg_resource_pool[Natts_pg_resource_pool] = {Schema_pg_resource_pool};
static const FormData_pg_attribute Desc_pg_workload_group[Natts_pg_workload_group] = {Schema_pg_workload_group};
static const FormData_pg_attribute Desc_pg_collation[Natts_pg_collation] = {Schema_pg_collation};
static const FormData_pg_attribute Desc_pg_auth_history[Natts_pg_auth_history] = {Schema_pg_auth_history};
static const FormData_pg_attribute Desc_pg_app_workloadgroup_mapping[Natts_pg_app_workloadgroup_mapping] = {
    Schema_pg_app_workloadgroup_mapping};
static const FormData_pg_attribute Desc_pg_enum[Natts_pg_enum] = {Schema_pg_enum};
static const FormData_pg_attribute Desc_pg_range[Natts_pg_range] = {Schema_pg_range};
static const FormData_pg_attribute Desc_pg_shseclabel[Natts_pg_shseclabel] = {Schema_pg_shseclabel};
static const FormData_pg_attribute Desc_pg_seclabel[Natts_pg_seclabel] = {Schema_pg_seclabel};
static const FormData_pg_attribute Desc_pg_ts_dict[Natts_pg_ts_dict] = {Schema_pg_ts_dict};
static const FormData_pg_attribute Desc_pg_ts_parser[Natts_pg_ts_parser] = {Schema_pg_ts_parser};
static const FormData_pg_attribute Desc_pg_ts_config[Natts_pg_ts_config] = {Schema_pg_ts_config};
static const FormData_pg_attribute Desc_pg_ts_config_map[Natts_pg_ts_config_map] = {Schema_pg_ts_config_map};
static const FormData_pg_attribute Desc_pg_ts_template[Natts_pg_ts_template] = {Schema_pg_ts_template};
static const FormData_pg_attribute Desc_pg_extension_data_source[Natts_pg_extension_data_source] = {
    Schema_pg_extension_data_source};
static const FormData_pg_attribute Desc_pg_directory[Natts_pg_directory] = {Schema_pg_directory};
static const FormData_pg_attribute Desc_pg_obsscaninfo[Natts_pg_obsscaninfo] = {Schema_pg_obsscaninfo};
static const FormData_pg_attribute Desc_pgxc_class[Natts_pgxc_class] = {Schema_pgxc_class};
static const FormData_pg_attribute Desc_pgxc_group[Natts_pgxc_group] = {Schema_pgxc_group};
static const FormData_pg_attribute Desc_pgxc_node[Natts_pgxc_node] = {Schema_pgxc_node};
static const FormData_pg_attribute Desc_pg_partition[Natts_pg_partition] = {Schema_pg_partition};
static const FormData_pg_attribute Desc_pg_job[Natts_pg_job] = {Schema_pg_job};
static const FormData_pg_attribute Desc_pg_job_proc[Natts_pg_job_proc] = {Schema_pg_job_proc};
static const FormData_pg_attribute Desc_gs_job_argument[Natts_gs_job_argument] = {Schema_gs_job_argument};
static const FormData_pg_attribute Desc_gs_job_attribute[Natts_gs_job_attribute] = {Schema_gs_job_attribute};
static const FormData_pg_attribute Desc_pg_object[Natts_pg_object] = {Schema_pg_object};
static const FormData_pg_attribute Desc_pg_synonym[Natts_pg_synonym] = {Schema_pg_synonym};
static const FormData_pg_attribute Desc_pg_hashbucket[Natts_pg_hashbucket] = {Schema_pg_hashbucket};
static const FormData_pg_attribute Desc_gs_uid[Natts_gs_uid] = {Schema_gs_uid};
static const FormData_pg_attribute Desc_pg_snapshot[Natts_pg_snapshot] = {Schema_gs_txn_snapshot};
static const FormData_pg_attribute Desc_pg_recyclebin[Natts_pg_recyclebin] = {Schema_gs_recyclebin};
static const FormData_pg_attribute Desc_gs_global_chain[Natts_gs_global_chain] = {Schema_gs_global_chain};
static const FormData_pg_attribute Desc_gs_global_config[Natts_gs_global_config] = {Schema_gs_global_config};
static const FormData_pg_attribute Desc_streaming_stream[Natts_streaming_stream] = {Schema_streaming_stream};
static const FormData_pg_attribute Desc_streaming_cont_query[Natts_streaming_cont_query] = {Schema_streaming_cont_query};
static const FormData_pg_attribute Desc_streaming_reaper_status[Natts_streaming_reaper_status] = \
{Schema_streaming_reaper_status};
static const FormData_pg_attribute Desc_gs_policy_label[Natts_gs_policy_label] = {Schema_gs_policy_label};
static const FormData_pg_attribute Desc_gs_auditing_policy[Natts_gs_auditing_policy] = {Schema_gs_auditing_policy};
static const FormData_pg_attribute Desc_gs_auditing_policy_acc[Natts_gs_auditing_policy_acc] = {Schema_gs_auditing_policy_access};
static const FormData_pg_attribute Desc_gs_auditing_policy_filter[Natts_gs_auditing_policy_filters] = {Schema_gs_auditing_policy_filters};
static const FormData_pg_attribute Desc_gs_auditing_policy_priv[Natts_gs_auditing_policy_priv] = {Schema_gs_auditing_policy_privileges};
static const FormData_pg_attribute Desc_gs_masking_policy[Natts_gs_masking_policy] = {Schema_gs_masking_policy};
static const FormData_pg_attribute Desc_gs_masking_policy_actions[Natts_gs_masking_policy_actions] = {Schema_gs_masking_policy_actions};
static const FormData_pg_attribute Desc_gs_masking_policy_filters[Natts_gs_masking_policy_filters] = {Schema_gs_masking_policy_filters};
static const FormData_pg_attribute Desc_gs_asp[Natts_gs_asp] = {Schema_gs_asp};
static const FormData_pg_attribute Desc_gs_matview[Natts_gs_matview] = {Schema_gs_matview};
static const FormData_pg_attribute Desc_gs_matview_dependency[Natts_gs_matview_dependency] = {Schema_gs_matview_dependency};
static const FormData_pg_attribute Desc_pgxc_slice[Natts_pgxc_slice] = {Schema_pgxc_slice};

static const FormData_pg_attribute Desc_gs_column_keys[Natts_gs_column_keys] = {Schema_gs_column_keys};
static const FormData_pg_attribute Desc_gs_column_keys_args[Natts_gs_column_keys_args] = {Schema_gs_column_keys_args};
static const FormData_pg_attribute Desc_gs_encrypted_columns[Natts_gs_encrypted_columns] = {Schema_gs_encrypted_columns};
static const FormData_pg_attribute Desc_gs_client_global_keys[Natts_gs_client_global_keys] = {Schema_gs_client_global_keys};
static const FormData_pg_attribute Desc_gs_client_global_keys_args[Natts_gs_client_global_keys_args] = {Schema_gs_client_global_keys_args};
static const FormData_pg_attribute Desc_gs_encrypted_proc[Natts_gs_encrypted_proc] = {Schema_gs_encrypted_proc};

static const FormData_pg_attribute Desc_gs_opt_model[Natts_gs_opt_model] = {Schema_gs_opt_model};
static const FormData_pg_attribute Desc_gs_model_warehouse[Natts_gs_model_warehouse] = {Schema_gs_model_warehouse};

static const FormData_pg_attribute Desc_gs_package[Natts_gs_package] = {Schema_gs_package};
static const FormData_pg_attribute Desc_gs_db_privilege[Natts_gs_db_privilege] = {Schema_gs_db_privilege};
static const FormData_pg_attribute Desc_pg_subscription[Natts_pg_subscription] = {Schema_pg_subscription};
static const FormData_pg_attribute Desc_pg_publication[Natts_pg_publication] = {Schema_pg_publication};
static const FormData_pg_attribute Desc_pg_publication_rel[Natts_pg_publication_rel] = {Schema_pg_publication_rel};
static const FormData_pg_attribute Desc_pg_replication_origin[Natts_pg_replication_origin] = {
    Schema_pg_replication_origin
};

/* Please add to the array in ascending order of oid value */
static struct CatalogRelationBuildParam catalogBuildParam[CATALOG_NUM] = {{DefaultAclRelationId,
                                                                              "pg_default_acl",
                                                                              DefaultAclRelation_Rowtype_Id,
                                                                              false,
                                                                              true,
                                                                              Natts_pg_default_acl,
                                                                              Desc_pg_default_acl,
                                                                              false,
                                                                              true},
    {PLTemplateRelationId,
        "pg_pltemplate",
        PLTemplateRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_pltemplate,
        Desc_pg_pltemplate,
        false,
        true},
    {TableSpaceRelationId,
        "pg_tablespace",
        TableSpaceRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_tablespace,
        Desc_pg_tablespace,
        false,
        true},
    {SharedDependRelationId,
        "pg_shdepend",
        SharedDependRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_shdepend,
        Desc_pg_shdepend,
        false,
        true},
    {TypeRelationId, "pg_type", TypeRelation_Rowtype_Id, false, true, Natts_pg_type, Desc_pg_type, true, true},
    {AttributeRelationId,
        "pg_attribute",
        AttributeRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_attribute,
        Desc_pg_attribute,
        true,
        true},
    {ProcedureRelationId,
        "pg_proc",
        ProcedureRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_proc,
        Desc_pg_proc,
        true,
        true},
    {RelationRelationId,
        "pg_class",
        RelationRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_class,
        Desc_pg_class,
        true,
        true},
    {AuthIdRelationId, "pg_authid", AuthIdRelation_Rowtype_Id, true, true, Natts_pg_authid, Desc_pg_authid, true, true},
    {AuthMemRelationId,
        "pg_auth_members",
        AuthMemRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_auth_members,
        Desc_pg_auth_members,
        true,
        true},
    {DatabaseRelationId,
        "pg_database",
        DatabaseRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_database,
        Desc_pg_database,
        true,
        true},
    {ForeignServerRelationId,
        "pg_foreign_server",
        ForeignServerRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_foreign_server,
        Desc_pg_foreign_server,
        false,
        true},
    {UserMappingRelationId,
        "pg_user_mapping",
        UserMappingRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_user_mapping,
        Desc_pg_user_mapping,
        false,
        true},
    {ForeignDataWrapperRelationId,
        "pg_foreign_data_wrapper",
        ForeignDataWrapperRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_foreign_data_wrapper,
        Desc_pg_foreign_data_wrapper,
        false,
        true},
    {SharedDescriptionRelationId,
        "pg_shdescription",
        SharedDescriptionRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_shdescription,
        Desc_pg_shdescription,
        false,
        true},
    {AggregateRelationId,
        "pg_aggregate",
        AggregateRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_aggregate,
        Desc_pg_aggregate,
        false,
        true},
    {AccessMethodRelationId,
        "pg_am",
        AccessMethodRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_am,
        Desc_pg_am,
        false,
        true},
    {AccessMethodOperatorRelationId,
        "pg_amop",
        AccessMethodOperatorRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_amop,
        Desc_pg_amop,
        false,
        true},
    {AccessMethodProcedureRelationId,
        "pg_amproc",
        AccessMethodProcedureRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_amproc,
        Desc_pg_amproc,
        false,
        true},
    {AttrDefaultRelationId,
        "pg_attrdef",
        AttrDefaultRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_attrdef,
        Desc_pg_attrdef,
        false,
        true},
    {CastRelationId, "pg_cast", CastRelation_Rowtype_Id, false, true, Natts_pg_cast, Desc_pg_cast, false, true},
    {ConstraintRelationId,
        "pg_constraint",
        ConstraintRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_constraint,
        Desc_pg_constraint,
        false,
        true},
    {ConversionRelationId,
        "pg_conversion",
        ConversionRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_conversion,
        Desc_pg_conversion,
        false,
        true},
    {DependRelationId,
        "pg_depend",
        DependRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_depend,
        Desc_pg_depend,
        false,
        true},
    {DescriptionRelationId,
        "pg_description",
        DescriptionRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_description,
        Desc_pg_description,
        false,
        true},
    {IndexRelationId, "pg_index", IndexRelation_Rowtype_Id, false, false, Natts_pg_index, Desc_pg_index, false, true},
    {InheritsRelationId,
        "pg_inherits",
        InheritsRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_inherits,
        Desc_pg_inherits,
        false,
        true},
    {LanguageRelationId,
        "pg_language",
        LanguageRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_language,
        Desc_pg_language,
        false,
        true},
    {LargeObjectRelationId,
        "pg_largeobject",
        LargeObjectRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_largeobject,
        Desc_pg_largeobject,
        false,
        true},
    {NamespaceRelationId,
        "pg_namespace",
        NamespaceRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_namespace,
        Desc_pg_namespace,
        false,
        true},
    {OperatorClassRelationId,
        "pg_opclass",
        OperatorClassRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_opclass,
        Desc_pg_opclass,
        false,
        true},
    {OperatorRelationId,
        "pg_operator",
        OperatorRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_operator,
        Desc_pg_operator,
        false,
        true},
    {RewriteRelationId,
        "pg_rewrite",
        RewriteRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_rewrite,
        Desc_pg_rewrite,
        false,
        true},
    {StatisticRelationId,
        "pg_statistic",
        StatisticRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_statistic,
        Desc_pg_statistic,
        false,
        true},
    {TriggerRelationId,
        "pg_trigger",
        TriggerRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_trigger,
        Desc_pg_trigger,
        false,
        true},
    {OperatorFamilyRelationId,
        "pg_opfamily",
        OperatorFamilyRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_opfamily,
        Desc_pg_opfamily,
        false,
        true},
    {DbRoleSettingRelationId,
        "pg_db_role_setting",
        DbRoleSettingRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_db_role_setting,
        Desc_pg_db_role_setting,
        false,
        true},
    {LargeObjectMetadataRelationId,
        "pg_largeobject_metadata",
        LargeObjectMetadataRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_largeobject_metadata,
        Desc_pg_largeobject_metadata,
        false,
        true},
    {ExtensionRelationId,
        "pg_extension",
        ExtensionRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_extension,
        Desc_pg_extension,
        false,
        true},
    {ForeignTableRelationId,
        "pg_foreign_table",
        ForeignTableRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_foreign_table,
        Desc_pg_foreign_table,
        false,
        true},
    {StatisticExtRelationId,
        "pg_statistic_ext",
        StatisticExtRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_statistic_ext,
        Desc_pg_statistic_ext,
        false,
        true},
    {RlsPolicyRelationId,
        "pg_rlspolicy",
        RlsPolicyRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_rlspolicy,
        Desc_pg_rlspolicy,
        false,
        true},
    {ResourcePoolRelationId,
        "pg_resource_pool",
        ResourcePoolRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_resource_pool,
        Desc_pg_resource_pool,
        false,
        true},
    {WorkloadGroupRelationId,
        "pg_workload_group",
        WorkloadGroupRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_workload_group,
        Desc_pg_workload_group,
        false,
        true},
    {CollationRelationId,
        "pg_collation",
        CollationRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_collation,
        Desc_pg_collation,
        false,
        true},
    {AuthHistoryRelationId,
        "pg_auth_history",
        AuthHistoryRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_auth_history,
        Desc_pg_auth_history,
        false,
        true},
    {UserStatusRelationId,
        "pg_user_status",
        UserStatusRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_user_status,
        Desc_pg_user_status,
        true,
        true},
    {AppWorkloadGroupMappingRelationId,
        "pg_app_workloadgroup_mapping",
        AppWorkloadGroupMappingRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_app_workloadgroup_mapping,
        Desc_pg_app_workloadgroup_mapping,
        false,
        true},
    {EnumRelationId, "pg_enum", EnumRelation_Rowtype_Id, false, true, Natts_pg_enum, Desc_pg_enum, false, true},
    {RangeRelationId, "pg_range", RangeRelation_Rowtype_Id, false, false, Natts_pg_range, Desc_pg_range, false, true},
    {PgSynonymRelationId,
        "pg_synonym",
        PgSynonymRelationId_Rowtype_Id,
        false,
        true,
        Natts_pg_synonym,
        Desc_pg_synonym,
        false,
        true},
    {SharedSecLabelRelationId,
        "pg_shseclabel",
        SharedSecLabelRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_shseclabel,
        Desc_pg_shseclabel,
        false,
        true},
    {SecLabelRelationId,
        "pg_seclabel",
        SecLabelRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_seclabel,
        Desc_pg_seclabel,
        false,
        true},
    {TSDictionaryRelationId,
        "pg_ts_dict",
        TSDictionaryRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_ts_dict,
        Desc_pg_ts_dict,
        false,
        true},
    {TSParserRelationId,
        "pg_ts_parser",
        TSParserRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_ts_parser,
        Desc_pg_ts_parser,
        false,
        true},
    {TSConfigRelationId,
        "pg_ts_config",
        TSConfigRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_ts_config,
        Desc_pg_ts_config,
        false,
        true},
    {TSConfigMapRelationId,
        "pg_ts_config_map",
        TSConfigMapRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_ts_config_map,
        Desc_pg_ts_config_map,
        false,
        true},
    {TSTemplateRelationId,
        "pg_ts_template",
        TSTemplateRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_ts_template,
        Desc_pg_ts_template,
        false,
        true},
    {ModelRelationId,
        "gs_model_warehouse",
        ModelRelation_Rowtype_Id,
        false,
        true,
        Natts_gs_model_warehouse,
        Desc_gs_model_warehouse,
        false,
        true},
    {DataSourceRelationId,
        "pg_extension_data_source",
        DataSourceRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_extension_data_source,
        Desc_pg_extension_data_source,
        false,
        true},
    {PgDirectoryRelationId,
        "pg_directory",
        PgDirectoryRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_directory,
        Desc_pg_directory,
        false,
        true},
    {DbPrivilegeId,
        "gs_db_privilege",
        DbPrivilege_Rowtype_Id,
        false,
        true,
        Natts_gs_db_privilege,
        Desc_gs_db_privilege,
        false,
        true},
    {ObsScanInfoRelationId,
        "pg_obsscaninfo",
        ObsScanInfoRelation_Rowtype_Id,
        false,
        false,
        Natts_pg_obsscaninfo,
        Desc_pg_obsscaninfo,
        false,
        true},
    {GsGlobalChainRelationId,
        "gs_global_chain",
        GsGlobalChainRelationId_Rowtype_Id,
        false,
        false,
        Natts_gs_global_chain,
        Desc_gs_global_chain,
        false,
        true},
    {SubscriptionRelationId,
        "pg_subscription",
        SubscriptionRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_subscription,
        Desc_pg_subscription,
        false,
        true},
    {PublicationRelationId,
        "pg_publication",
        PublicationRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_publication,
        Desc_pg_publication,
        false,
        true},
    {PublicationRelRelationId,
        "pg_publication_rel",
        PublicationRelRelationId_Rowtype_Id,
        false,
        true,
        Natts_pg_publication_rel,
        Desc_pg_publication_rel,
        false,
        true},
    {ReplicationOriginRelationId,
        "pg_replication_origin",
        ReplicationOriginRelationId_Rowtype_Id,
        true,
        false,
        Natts_pg_replication_origin,
        Desc_pg_replication_origin,
        false,
        true},
    {PackageRelationId,
        "gs_package",
        PackageRelation_Rowtype_Id,
        false,
        true,
        Natts_gs_package,
        Desc_gs_package,
        false,
        true},
    {RecyclebinRelationId,
        "gs_recyclebin",
        RecyclebinRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_recyclebin,
        Desc_pg_recyclebin,
        false,
        true},
    {SnapshotRelationId,
        "gs_txn_snapshot",
        SnapshotRelation_Rowtype_Id,
        true,
        false,
        Natts_pg_snapshot,
        Desc_pg_snapshot,
        false,
        true},
    {UidRelationId,
        "gs_uid",
        UidRelationId_Rowtype_Id,
        false,
        false,
        Natts_gs_uid,
        Desc_gs_uid,
        false,
        true},
    {PgxcClassRelationId,
        "pgxc_class",
        PgxcClassRelation_Rowtype_Id,
        false,
        false,
        Natts_pgxc_class,
        Desc_pgxc_class,
        false,
        true},
    {PgxcGroupRelationId,
        "pgxc_group",
        PgxcGroupRelation_Rowtype_Id,
        true,
        true,
        Natts_pgxc_group,
        Desc_pgxc_group,
        false,
        true},
    {PgxcNodeRelationId,
        "pgxc_node",
        PgxcNodeRelation_Rowtype_Id,
        true,
        true,
        Natts_pgxc_node,
        Desc_pgxc_node,
        false,
        true},
    {PartitionRelationId,
        "pg_partition",
        PartitionRelation_Rowtype_Id,
        false,
        true,
        Natts_pg_partition,
        Desc_pg_partition,
        false,
        true},
    {PgJobRelationId, "pg_job", PgJobRelation_Rowtype_Id, true, true, Natts_pg_job, Desc_pg_job, false, true},
    {PgJobProcRelationId,
        "pg_job_proc",
        PgJobProcRelation_Rowtype_Id,
        true,
        true,
        Natts_pg_job_proc,
        Desc_pg_job_proc,
        false,
        true},
    {PgObjectRelationId,
        "pg_object",
        PgObjectRelationId_Rowtype_Id,
        false,
        false,
        Natts_pg_object,
        Desc_pg_object,
        false,
        true},
    {HashBucketRelationId,
        "pg_hashbucket",
        HashBucketRelationId_Rowtype_Id,
        false,
        true,
        Natts_pg_hashbucket,
        Desc_pg_hashbucket,
        false,
        true},
    {StreamingStreamRelationId,
        "streaming_stream", 
        StreamingStreamRelation_Rowtype_Id,
        false,
        true,
        Natts_streaming_stream, 
        Desc_streaming_stream, 
        false,
        true},
    {StreamingContQueryRelationId,
        "streaming_cont_query", 
        StreamingContQueryRelation_Rowtype_Id,
        false,
        true,
        Natts_streaming_cont_query, 
        Desc_streaming_cont_query, 
        false, 
        true},
    {StreamingReaperStatusRelationId,
        "streaming_reaper_status", 
        StreamingReaperStatusRelation_Rowtype_Id,
        false,
        true,
        Natts_streaming_reaper_status, 
        Desc_streaming_reaper_status, 
        false,
        true},
    {GsJobAttributeRelationId,
        "gs_job_attribute",
        GsJobAttributeRelation_Rowtype_Id,
        false,
        true,
        Natts_gs_job_attribute,
        Desc_gs_job_attribute,
        false,
        true},
    {PgxcSliceRelationId,
        "pgxc_slice",
        PgxcSliceRelation_Rowtype_Id,
        false,
        false,
        Natts_pgxc_slice,
        Desc_pgxc_slice,
        false,
        true},
    {GsJobArgumentRelationId,
        "gs_job_argument",
        GsJobArgumentRelation_Rowtype_Id,
        false,
        true,
        Natts_gs_job_argument,
        Desc_gs_job_argument,
        false,
        true},
    {GsGlobalConfigRelationId,
        "gs_global_config",
        GsGlobalConfigRelationId_Rowtype_Id,
        true,
        false,
        Natts_gs_global_config,
        Desc_gs_global_config,
        false,
        true},
    {GsPolicyLabelRelationId,
        "gs_policy_label",
        GsPolicyLabelRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_policy_label,
        Desc_gs_policy_label,
        false,
        true},
    {GsAuditingPolicyRelationId,
        "gs_auditing_policy",
        GsAuditingPolicyRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_auditing_policy,
        Desc_gs_auditing_policy,
        false,
        true},
    {GsAuditingPolicyAccessRelationId,
        "gs_auditing_policy_acc",
        GsAuditingPolicyAccessRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_auditing_policy_acc,
        Desc_gs_auditing_policy_acc,
        false,
        true},
    {GsAuditingPolicyPrivilegesRelationId,
        "gs_auditing_policy_priv",
        GsAuditingPolicyPrivilegesRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_auditing_policy_priv,
        Desc_gs_auditing_policy_priv,
        false,
        true},
    {GsAspRelationId,
        "gs_asp",
        GsAspRelation_Rowtype_Id,
        false,
        false,
        Natts_gs_asp,
        Desc_gs_asp,
        false,
        true},
    {GsAuditingPolicyFiltersRelationId,
        "gs_auditing_policy_filter",
        GsAuditingPolicyFiltersRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_auditing_policy_filters,
        Desc_gs_auditing_policy_filter,
        false,
        true},
    {GsMaskingPolicyRelationId,
        "gs_masking_policy",
        GsMaskingPolicyRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_masking_policy,
        Desc_gs_masking_policy,
        false,
        true},
    {GsMaskingPolicyFiltersId,
        "gs_masking_policy_filters",
        GsMaskingPolicyFiltersId_Rowtype_Id,
        false,
        true,
        Natts_gs_masking_policy_filters,
        Desc_gs_masking_policy_filters,
        false,
        true},
    {GsMaskingPolicyActionsId,
        "gs_masking_policy_actions",
        GsMaskingPolicyActionsId_Rowtype_Id,
        false,
        true,
        Natts_gs_masking_policy_actions,
        Desc_gs_masking_policy_actions,
        false,
        true},
    {ClientLogicCachedColumnsId,
        "gs_encrypted_columns",
        ClientLogicCachedColumnsId_Rowtype_Id,
        false,
        true,
        Natts_gs_encrypted_columns,
        Desc_gs_encrypted_columns,
        false,
        true},
    {ClientLogicGlobalSettingsId,
        "gs_client_global_keys",
        ClientLogicGlobalSettingsId_Rowtype_Id,
        false,
        true,
        Natts_gs_client_global_keys,
        Desc_gs_client_global_keys,
        false,
        true},
    {ClientLogicColumnSettingsId,
        "gs_column_keys",
        ClientLogicColumnSettingsId_Rowtype_Id,
        false,
        true,
        Natts_gs_column_keys,
        Desc_gs_column_keys,
        false,
        true},
    {ClientLogicGlobalSettingsArgsId,
        "gs_client_global_keys_args",
        ClientLogicGlobalSettingsArgsId_Rowtype_Id,
        false,
        true,
        Natts_gs_client_global_keys_args,
        Desc_gs_client_global_keys_args,
        false,
        true},
    {ClientLogicColumnSettingsArgsId,
        "gs_column_keys_args",
        ClientLogicColumnSettingsArgsId_Rowtype_Id,
        false,
        true,
        Natts_gs_column_keys_args,
        Desc_gs_column_keys_args,
        false,
        true},
    {ClientLogicProcId,
        "gs_encrypted_proc",
        ClientLogicProcId_Rowtype_Id,
        false,
        true,
        Natts_gs_encrypted_proc,
        Desc_gs_encrypted_proc,
        false,
        true},
    {MatviewRelationId,
        "gs_matview",
        MatviewRelationId_Rowtype_Id,
        false,
        true,
        Natts_gs_matview,
        Desc_gs_matview,
        false,
        true},
    {MatviewDependencyId,
        "gs_matview_dependency",
        MatviewDependencyId_Rowtype_Id,
        false,
        true,
        Natts_gs_matview_dependency,
        Desc_gs_matview_dependency,
        false,
        true},
    {OptModelRelationId,
        "gs_opt_model",
        OptModelRelationId_Rowtype_Id,
        false,
        false,
        Natts_gs_opt_model,
        Desc_gs_opt_model,
        false,
        true}};

// Get cluster information of relation
//
static void ClusterConstraintFetch(__inout Relation relation);

/*
 *		Hash tables that index the relation cache
 *
 *		We used to index the cache by both name and OID, but now there
 *		is only an index by OID.
 */
typedef struct relidcacheent {
    Oid reloid;
    Relation reldesc;
} RelIdCacheEnt;

/*
 * This flag is false until we have hold the CriticalCacheBuildLock
 */
THR_LOCAL bool needNewLocalCacheFile = false;

/*
 * Special cache for opclass-related information
 *
 * Note: only default support procs get cached, ie, those with
 * lefttype = righttype = opcintype.
 */
typedef struct opclasscacheent {
    Oid opclassoid;             /* lookup key: OID of opclass */
    bool valid;                 /* set TRUE after successful fill-in */
    StrategyNumber numSupport;  /* max # of support procs (from pg_am) */
    Oid opcfamily;              /* OID of opclass's family */
    Oid opcintype;              /* OID of opclass's declared input type */
    RegProcedure* supportProcs; /* OIDs of support procedures */
} OpClassCacheEnt;

/* non-export function prototypes */
static void RememberToFreeTupleDescAtEOX(TupleDesc td);

static void RelationFlushRelation(Relation relation);
static bool load_relcache_init_file(bool shared);
static void write_relcache_init_file(bool shared);
static void write_item(const void* data, Size len, FILE* fp);

static Relation AllocateRelationDesc(Form_pg_class relp);
static void RelationParseRelOptions(Relation relation, HeapTuple tuple);
static void RelationBuildTupleDesc(Relation relation, bool onlyLoadInitDefVal);

static void RelationInitBucketKey(Relation relation, HeapTuple tuple);
static void RelationInitBucketInfo(Relation relation, HeapTuple tuple);

static void AttrDefaultFetch(Relation relation);
static void CheckConstraintFetch(Relation relation);
static List* insert_ordered_oid(List* list, Oid datum);
static void IndexSupportInitialize(Relation relation, oidvector* indclass, StrategyNumber maxSupportNumber,
    AttrNumber maxAttributeNumber);
static OpClassCacheEnt* LookupOpclassInfo(Relation relation, Oid operatorClassOid, StrategyNumber numSupport);
static void RelationCacheInitFileRemoveInDir(const char* tblspcpath);
static void unlink_initfile(const char* initfilename);
/*
 *		ScanPgRelation
 *
 *		This is used by RelationBuildDesc to find a pg_class
 *		tuple matching targetRelId.  The caller must hold at least
 *		AccessShareLock on the target relid to prevent concurrent-update
 *		scenarios --- else our SnapshotNow scan might fail to find any
 *		version that it thinks is live.
 *
 *		NB: the returned tuple has been copied into palloc'd storage
 *		and must eventually be freed with heap_freetuple.
 */
HeapTuple ScanPgRelation(Oid targetRelId, bool indexOK, bool force_non_historic)
{
    HeapTuple pg_class_tuple;
    Relation pg_class_desc;
    SysScanDesc pg_class_scan;
    ScanKeyData key[1];
    Snapshot snapshot = NULL;
    /*
     * If something goes wrong during backend startup, we might find ourselves
     * trying to read pg_class before we've selected a database.  That ain't
     * gonna work, so bail out with a useful error message.  If this happens,
     * it probably means a relcache entry that needs to be nailed isn't.
     */
    if (!OidIsValid(GetMyDatabaseId())) {
        ereport(FATAL, (errmsg("cannot read pg_class without having selected a database")));
    }

    /*
     * form a scan key
     */
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(targetRelId));

    /*
     * Open pg_class and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical relcache entries (this includes initdb and startup
     * without a pg_internal.init file).  The caller can also force a heap
     * scan by setting indexOK == false.
     */
    pg_class_desc = heap_open(RelationRelationId, AccessShareLock);

    /*
     * The caller might need a tuple that's newer than the one the historic
     * snapshot; currently the only case requiring to do so is looking up the
     * relfilenode of non mapped system relations during decoding.
     */
    snapshot = SnapshotNow;
    if (HistoricSnapshotActive() && !force_non_historic) {
        snapshot = GetCatalogSnapshot();
    }

    /*
     * The caller might need a tuple that's newer than the one the historic
     * snapshot; currently the only case requiring to do so is looking up the
     * relfilenode of non mapped system relations during decoding.
     */

    pg_class_scan = systable_beginscan(
        pg_class_desc, ClassOidIndexId, indexOK && LocalRelCacheCriticalRelcachesBuilt(), snapshot, 1, key);

#ifdef ENABLE_MULTIPLE_NODES
    /* If we do seqscan on pg_class, skip sync. */
    if (pg_class_scan->irel == NULL && snapshot == SnapshotNow) {
        pg_class_scan->scan->rs_base.rs_snapshot = SnapshotNowNoSync;
    }
#endif
    pg_class_tuple = systable_getnext(pg_class_scan);

    /*
     * Must copy tuple before releasing buffer.
     */
    if (HeapTupleIsValid(pg_class_tuple))
        pg_class_tuple = heap_copytuple(pg_class_tuple);

    /* all done */
    systable_endscan(pg_class_scan);
    heap_close(pg_class_desc, AccessShareLock);

    return pg_class_tuple;
}

/*
 *		AllocateRelationDesc
 *
 *		This is used to allocate memory for a new relation descriptor
 *		and initialize the rd_rel field from the given pg_class tuple.
 */
static Relation AllocateRelationDesc(Form_pg_class relp)
{
    Relation relation;
    MemoryContext oldcxt;
    Form_pg_class relationForm;

    /* Relcache entries must live in LocalMyDBCacheMemCxt */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /*
     * allocate and zero space for new relation descriptor
     */
    relation = (Relation)palloc0(sizeof(RelationData));

    /* make sure relation is marked as having no open file yet */
    relation->rd_smgr = NULL;

    /* init bucketkey to point itself means bucket key is not build yet */
    relation->rd_bucketkey = (RelationBucketKey *)&(relation->rd_bucketkey);
    relation->rd_bucketoid = InvalidOid;

    /*
     * Copy the relation tuple form
     *
     * We only allocate space for the fixed fields, ie, CLASS_TUPLE_SIZE. The
     * variable-length fields (relacl, reloptions) are NOT stored in the
     * relcache --- there'd be little point in it, since we don't copy the
     * tuple's nulls bitmap and hence wouldn't know if the values are valid.
     * Bottom line is that relacl *cannot* be retrieved from the relcache. Get
     * it from the syscache if you need it.  The same goes for the original
     * form of reloptions (however, we do store the parsed form of reloptions
     * in rd_options).
     */
    relationForm = (Form_pg_class)palloc(sizeof(FormData_pg_class));

    errno_t rc = memcpy_s(relationForm, CLASS_TUPLE_SIZE, relp, CLASS_TUPLE_SIZE);
    securec_check(rc, "", "");

    /* initialize relation tuple form */
    relation->rd_rel = relationForm;

    /* and allocate attribute tuple form storage */
    relation->rd_att = CreateTemplateTupleDesc(relationForm->relnatts, relationForm->relhasoids, TAM_INVALID);
    //TODO:this should be TAM_invalid when merge ustore
    relation->rd_tam_type = TAM_HEAP;
    /* which we mark as a reference-counted tupdesc */
    relation->rd_att->tdrefcount = 1;

    (void)MemoryContextSwitchTo(oldcxt);

    return relation;
}

/*
 * RelationParseRelOptions
 *		Convert pg_class.reloptions into pre-parsed rd_options
 *
 * tuple is the real pg_class tuple (not rd_rel!) for relation
 *
 * Note: rd_rel and (if an index) rd_am must be valid already
 */
static void RelationParseRelOptions(Relation relation, HeapTuple tuple)
{
    bytea* options = NULL;

    relation->rd_options = NULL;

    /* Fall out if relkind should not have options */
    switch (relation->rd_rel->relkind) {
        case RELKIND_RELATION:
        case RELKIND_TOASTVALUE:
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
        case RELKIND_MATVIEW:
            break;
        default:
            return;
    }

    /*
     * Fetch reloptions from tuple; have to use a hardwired descriptor because
     * we might not have any other for pg_class yet (consider executing this
     * code for pg_class itself)
     */
    options = extractRelOptions(tuple, GetLSCPgClassDescriptor(),
        RelationIsIndex(relation) ? relation->rd_am->amoptions : InvalidOid);
    /*
     * Copy parsed data into LocalMyDBCacheMemCxt.  To guard against the
     * possibility of leaks in the reloptions code, we want to do the actual
     * parsing in the caller's memory context and copy the results into
     * LocalMyDBCacheMemCxt after the fact.
     */
    if (options != NULL) {
        relation->rd_options = (bytea*)MemoryContextAlloc(LocalMyDBCacheMemCxt(), VARSIZE(options));
        errno_t rc = memcpy_s(relation->rd_options, VARSIZE(options), options, VARSIZE(options));
        securec_check(rc, "", "");
        pfree_ext(options);
    }
}

/*
 *		RelationBuildTupleDesc
 *
 *		Form the relation's tuple descriptor from information in
 *		the pg_attribute, pg_attrdef & pg_constraint system catalogs.
 *
 *		For catalog relcache built from schemapg.h or init file, we
 *		need to load their initial default values separately.
 */
static void RelationBuildTupleDesc(Relation relation, bool onlyLoadInitDefVal)
{
    HeapTuple pg_attribute_tuple;
    Relation pg_attribute_desc;
    SysScanDesc pg_attribute_scan;
    ScanKeyData skey[2];
    int need;
    TupleConstr* constr = NULL;
    AttrDefault* attrdef = NULL;
    int ndef = 0;

    /* alter table instantly */
    Datum dval;
    bool isNull = false;
    bool hasInitDefval = false;
    TupInitDefVal* initdvals = NULL;

    if (!onlyLoadInitDefVal) {
        /* copy some fields from pg_class row to rd_att */
        relation->rd_att->tdtypeid = relation->rd_rel->reltype;
        relation->rd_att->tdtypmod = -1; /* unnecessary, but... */
        relation->rd_att->tdhasoid = relation->rd_rel->relhasoids;

        constr = (TupleConstr*)MemoryContextAllocZero(LocalMyDBCacheMemCxt(), sizeof(TupleConstr));
        constr->has_not_null = false;
        constr->has_generated_stored = false;
    }

    /*
     * Form a scan key that selects only user attributes (attnum > 0).
     * (Eliminating system attribute rows at the index level is lots faster
     * than fetching them.)
     */
    ScanKeyInit(&skey[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)));
    ScanKeyInit(&skey[1], Anum_pg_attribute_attnum, BTGreaterStrategyNumber, F_INT2GT, Int16GetDatum(0));

    /*
     * Using historical snapshot in logic decoding.
     */
    Snapshot snapshot = NULL;
    snapshot = SnapshotNow;
    if (HistoricSnapshotActive()) {
        snapshot =  GetCatalogSnapshot();
    }

    /*
     * Open pg_attribute and begin a scan.	Force heap scan if we haven't yet
     * built the critical relcache entries (this includes initdb and startup
     * without a pg_internal.init file).
     */
    pg_attribute_desc = heap_open(AttributeRelationId, AccessShareLock);
    pg_attribute_scan = systable_beginscan(
        pg_attribute_desc, AttributeRelidNumIndexId, LocalRelCacheCriticalRelcachesBuilt(), snapshot, 2, skey);

    /*
     * add attribute data to relation->rd_att
     */
    need = RelationGetNumberOfAttributes(relation);

    /* alter table instantly or load catalog init default during backend startup */
    Assert(relation->rd_att->initdefvals == NULL || onlyLoadInitDefVal);

    if (relation->rd_att->initdefvals != NULL && onlyLoadInitDefVal)
        return;

    /* set all the *TupInitDefVal* objects later. */
    initdvals = (TupInitDefVal*)MemoryContextAllocZero(LocalMyDBCacheMemCxt(), need * sizeof(TupInitDefVal));

    while (HeapTupleIsValid(pg_attribute_tuple = systable_getnext(pg_attribute_scan))) {
        Form_pg_attribute attp;

        attp = (Form_pg_attribute)GETSTRUCT(pg_attribute_tuple);

        if (attp->attnum <= 0 || attp->attnum > RelationGetNumberOfAttributes(relation))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid attribute number %d for %s", attp->attnum, RelationGetRelationName(relation))));

        if (!onlyLoadInitDefVal) {
            /*
             * Check if the attribute has been updated.
             * Some modification can change relation attributes concurrently
             * without holding any relation lock. Currently, we use snapshot satisifyNow
             * to decide the visibility of tuples, so it can see the different version of tuples 
             * of the same hot update chain.
             * e.g: grant xxx on table to user1. revoke xxx on table from user1.
             * Since relation->rd_att->atts palloc0 in function CreateTemplateTupleDesc,
             * its attnum should be zero, use it to check if above scenario happened.
             */
            if (relation->rd_att->attrs[attp->attnum - 1]->attnum != 0) {
                /* Panic if we hit here many times, plus 2 make sure it has enough room in err stack */
                int eLevel = ((t_thrd.log_cxt.errordata_stack_depth + 2) < ERRORDATA_STACK_SIZE) ? ERROR : PANIC;
                ereport(eLevel,(errmsg("Catalog attribute %d for relation \"%s\" has been updated concurrently",
                    attp->attnum, RelationGetRelationName(relation))));
            }
            errno_t rc = memcpy_s(
                relation->rd_att->attrs[attp->attnum - 1], ATTRIBUTE_FIXED_PART_SIZE, attp, ATTRIBUTE_FIXED_PART_SIZE);
            securec_check(rc, "\0", "\0");
        }
        if (initdvals != NULL) {
            dval = fastgetattr(pg_attribute_tuple, Anum_pg_attribute_attinitdefval, pg_attribute_desc->rd_att, &isNull);

            if (isNull) {
                initdvals[attp->attnum - 1].isNull = true;
                initdvals[attp->attnum - 1].datum = NULL;
                initdvals[attp->attnum - 1].dataLen = 0;
            } else {
                /* fetch and copy the default value. */
                bytea* val = DatumGetByteaP(dval);
                int len = VARSIZE(val) - VARHDRSZ;
                char* buf = (char*)MemoryContextAlloc(LocalMyDBCacheMemCxt(), len);
                errno_t rc = memcpy_s(buf, len, VARDATA(val), len);
                securec_check(rc, "", "");

                initdvals[attp->attnum - 1].isNull = false;
                initdvals[attp->attnum - 1].datum = (Datum*)buf;
                initdvals[attp->attnum - 1].dataLen = len;
                hasInitDefval = true;
            }
        }

        /* Update constraint/default info */
        if (attp->attnotnull && !onlyLoadInitDefVal)
            constr->has_not_null = true;

        if (attp->atthasdef && !onlyLoadInitDefVal) {
            if (attrdef == NULL)
                attrdef = (AttrDefault*)MemoryContextAllocZero(
                    LocalMyDBCacheMemCxt(), RelationGetNumberOfAttributes(relation) * sizeof(AttrDefault));
            attrdef[ndef].adnum = attp->attnum;
            attrdef[ndef].adbin = NULL;
            ndef++;
        }
        need--;
        if (need == 0)
            break;
    }

    /*
     * end the scan and close the attribute relation
     */
    systable_endscan(pg_attribute_scan);
    heap_close(pg_attribute_desc, AccessShareLock);

    if (need != 0) {
        /* find all missed attributes, and print them */
        StringInfo missing_attnums = makeStringInfo();
        for (int i = 0; i <  RelationGetNumberOfAttributes(relation); i++) {
            if (0 == relation->rd_att->attrs[i]->attnum) {
                appendStringInfo(missing_attnums, "%d ", (i + 1));
            }
        }
        ereport(ERROR,
            (errcode(ERRCODE_NO_DATA),
                errmsg("Catalog is missing %d attribute(s) for relid %u", need, RelationGetRelid(relation)),
                errdetail("Relation \"%s\" expects %d attribute(s), but not found [%s]",
                    RelationGetRelationName(relation),
                    relation->rd_rel->relnatts,
                    missing_attnums->data)));
    }

    /*
     * if this relation doesn't have any alter-table-instantly data,
     * free and reset *initdefvals* to be null.
     */
    if (initdvals != NULL && !hasInitDefval)
        pfree_ext(initdvals);
    else if (initdvals != NULL && relation->rd_att->initdefvals != NULL) {
        for (int i = 0; i < RelationGetNumberOfAttributes(relation); ++i) {
            if (initdvals[i].datum != NULL)
                pfree_ext(initdvals[i].datum);
        }
        pfree_ext(initdvals);
    } else
        relation->rd_att->initdefvals = initdvals;

    if (onlyLoadInitDefVal)
        return;
        /*
         * The attcacheoff values we read from pg_attribute should all be -1
         * ("unknown").  Verify this if assert checking is on.	They will be
         * computed when and if needed during tuple access.
         *
         * If we are separately loading catalog relcache initial default, their
         * attcacheoff may have been updated. In such case, skip assertation.
         */
#ifdef USE_ASSERT_CHECKING
    {
        int i;

        for (i = 0; i < RelationGetNumberOfAttributes(relation); i++)
            Assert(relation->rd_att->attrs[i]->attcacheoff == -1);
    }
#endif

    /*
     * However, we can easily set the attcacheoff value for the first
     * attribute: it must be zero.	This eliminates the need for special cases
     * for attnum=1 that used to exist in fastgetattr() and index_getattr().
     */
    if (!RelationIsUstoreFormat(relation) && RelationGetNumberOfAttributes(relation) > 0)
        relation->rd_att->attrs[0]->attcacheoff = 0;

    /*
     * Set up constraint/default info
     */
    if (constr->has_not_null || ndef > 0 || relation->rd_rel->relchecks || relation->rd_rel->relhasclusterkey) {
        relation->rd_att->constr = constr;

        if (ndef > 0) /* DEFAULTs */
        {
            if (ndef < RelationGetNumberOfAttributes(relation))
                constr->defval = (AttrDefault*)repalloc(attrdef, ndef * sizeof(AttrDefault));
            else
                constr->defval = attrdef;
            constr->num_defval = ndef;
            constr->generatedCols = (char *)MemoryContextAllocZero(LocalMyDBCacheMemCxt(),
                RelationGetNumberOfAttributes(relation) * sizeof(char));
            AttrDefaultFetch(relation);
        } else {
            constr->num_defval = 0;
            constr->defval = NULL;
            constr->generatedCols = NULL;
        }

        if (relation->rd_rel->relchecks > 0) /* CHECKs */
        {
            constr->num_check = relation->rd_rel->relchecks;
            constr->check =
                (ConstrCheck*)MemoryContextAllocZero(LocalMyDBCacheMemCxt(), constr->num_check * sizeof(ConstrCheck));
            CheckConstraintFetch(relation);
        } else {
            constr->num_check = 0;
            constr->check = NULL;
        }

        /* Relation has cluster keys */
        if (relation->rd_rel->relhasclusterkey) {
            ClusterConstraintFetch(relation);
        } else {
            constr->clusterKeyNum = 0;
            constr->clusterKeys = NULL;
        }
    } else {
        pfree_ext(constr);
        relation->rd_att->constr = NULL;
    }
}

/*
 *		RelationBuildRuleLock
 *
 *		Form the relation's rewrite rules from information in
 *		the pg_rewrite system catalog.
 *
 * Note: The rule parsetrees are potentially very complex node structures.
 * To allow these trees to be freed when the relcache entry is flushed,
 * we make a private memory context to hold the RuleLock information for
 * each relcache entry that has associated rules.  The context is used
 * just for rule info, not for any other subsidiary data of the relcache
 * entry, because that keeps the update logic in RelationClearRelation()
 * manageable.	The other subsidiary data structures are simple enough
 * to be easy to free explicitly, anyway.
 */
void RelationBuildRuleLock(Relation relation)
{
    MemoryContext rulescxt;
    MemoryContext oldcxt;
    HeapTuple rewrite_tuple;
    Relation rewrite_desc;
    TupleDesc rewrite_tupdesc;
    SysScanDesc rewrite_scan;
    ScanKeyData key;
    RuleLock* rulelock = NULL;
    int numlocks;
    RewriteRule** rules;
    int maxlocks;

    /*
     * Make the private context.  Parameters are set on the assumption that
     * it'll probably not contain much data.
     */

    rulescxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(),
        RelationGetRelationName(relation),
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
    
    relation->rd_rulescxt = rulescxt;

    /*
     * allocate an array to hold the rewrite rules (the array is extended if
     * necessary)
     */
    maxlocks = 4;
    rules = (RewriteRule**)MemoryContextAlloc(rulescxt, sizeof(RewriteRule*) * maxlocks);
    numlocks = 0;

    /*
     * form a scan key
     */
    ScanKeyInit(
        &key, Anum_pg_rewrite_ev_class, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    /*
     * open pg_rewrite and begin a scan
     *
     * Note: since we scan the rules using RewriteRelRulenameIndexId, we will
     * be reading the rules in name order, except possibly during
     * emergency-recovery operations (ie, IgnoreSystemIndexes). This in turn
     * ensures that rules will be fired in name order.
     */
    rewrite_desc = heap_open(RewriteRelationId, AccessShareLock);
    rewrite_tupdesc = RelationGetDescr(rewrite_desc);
    rewrite_scan = systable_beginscan(rewrite_desc, RewriteRelRulenameIndexId, true, NULL, 1, &key);

    while (HeapTupleIsValid(rewrite_tuple = systable_getnext(rewrite_scan))) {
        Form_pg_rewrite rewrite_form = (Form_pg_rewrite)GETSTRUCT(rewrite_tuple);
        bool isnull = false;
        Datum rule_datum;
        char* rule_str = NULL;
        RewriteRule* rule = NULL;

        rule = (RewriteRule*)MemoryContextAlloc(rulescxt, sizeof(RewriteRule));

        rule->ruleId = HeapTupleGetOid(rewrite_tuple);

        rule->event = (CmdType)(rewrite_form->ev_type - '0');
        rule->attrno = rewrite_form->ev_attr;
        rule->enabled = rewrite_form->ev_enabled;
        rule->isInstead = rewrite_form->is_instead;

        /*
         * Must use heap_getattr to fetch ev_action and ev_qual.  Also, the
         * rule strings are often large enough to be toasted.  To avoid
         * leaking memory in the caller's context, do the detoasting here so
         * we can free the detoasted version.
         */
        rule_datum = heap_getattr(rewrite_tuple, Anum_pg_rewrite_ev_action, rewrite_tupdesc, &isnull);
        Assert(!isnull);
        rule_str = TextDatumGetCString(rule_datum);
        oldcxt = MemoryContextSwitchTo(rulescxt);
        rule->actions = (List*)stringToNode(rule_str);
        (void)MemoryContextSwitchTo(oldcxt);
        pfree_ext(rule_str);

        rule_datum = heap_getattr(rewrite_tuple, Anum_pg_rewrite_ev_qual, rewrite_tupdesc, &isnull);
        Assert(!isnull);
        rule_str = TextDatumGetCString(rule_datum);
        oldcxt = MemoryContextSwitchTo(rulescxt);
        rule->qual = (Node*)stringToNode(rule_str);
        (void)MemoryContextSwitchTo(oldcxt);
        pfree_ext(rule_str);

        /*
         * We want the rule's table references to be checked as though by the
         * table owner, not the user referencing the rule.	Therefore, scan
         * through the rule's actions and set the checkAsUser field on all
         * rtable entries.	We have to look at the qual as well, in case it
         * contains sublinks.
         *
         * The reason for doing this when the rule is loaded, rather than when
         * it is stored, is that otherwise ALTER TABLE OWNER would have to
         * grovel through stored rules to update checkAsUser fields. Scanning
         * the rule tree during load is relatively cheap (compared to
         * constructing it in the first place), so we do it here.
         */
        setRuleCheckAsUser((Node*)rule->actions, relation->rd_rel->relowner);
        setRuleCheckAsUser(rule->qual, relation->rd_rel->relowner);

        if (numlocks >= maxlocks) {
            maxlocks *= 2;
            rules = (RewriteRule**)repalloc(rules, sizeof(RewriteRule*) * maxlocks);
        }
        rules[numlocks++] = rule;
    }

    /*
     * end the scan and close the attribute relation
     */
    systable_endscan(rewrite_scan);
    heap_close(rewrite_desc, AccessShareLock);

    /*
     * there might not be any rules (if relhasrules is out-of-date)
     */
    if (numlocks == 0) {
        relation->rd_rules = NULL;
        relation->rd_rulescxt = NULL;
        MemoryContextDelete(rulescxt);
        return;
    }

    /*
     * form a RuleLock and insert into relation
     */
    rulelock = (RuleLock*)MemoryContextAlloc(rulescxt, sizeof(RuleLock));
    rulelock->numLocks = numlocks;
    rulelock->rules = rules;

    relation->rd_rules = rulelock;
}

/*
 *		equalRuleLocks
 *
 *		Determine whether two RuleLocks are equivalent
 *
 *		Probably this should be in the rules code someplace...
 */
static bool equalRuleLocks(RuleLock* rlock1, RuleLock* rlock2)
{
    int i;

    /*
     * As of 7.3 we assume the rule ordering is repeatable, because
     * RelationBuildRuleLock should read 'em in a consistent order.  So just
     * compare corresponding slots.
     */
    if (rlock1 != NULL) {
        if (rlock2 == NULL)
            return false;
        if (rlock1->numLocks != rlock2->numLocks)
            return false;
        for (i = 0; i < rlock1->numLocks; i++) {
            RewriteRule* rule1 = rlock1->rules[i];
            RewriteRule* rule2 = rlock2->rules[i];

            if (rule1->ruleId != rule2->ruleId)
                return false;
            if (rule1->event != rule2->event)
                return false;
            if (rule1->attrno != rule2->attrno)
                return false;
            if (rule1->enabled != rule2->enabled)
                return false;
            if (rule1->isInstead != rule2->isInstead)
                return false;
            if (!equal(rule1->qual, rule2->qual))
                return false;
            if (!equal(rule1->actions, rule2->actions))
                return false;
        }
    } else if (rlock2 != NULL)
        return false;
    return true;
}

static Relation CatalogRelationBuildDesc(const char* relationName, Oid relationReltype, bool isshared, bool hasoids,
    int natts, const FormData_pg_attribute* attrs, bool isnailed, bool insertIt)
{
    Relation relation;
    int i;
    bool has_not_null = false;
    MemoryContext oldcxt;
    /* Relcache entries must live in t_thrd.mem_cxt.cache_mem_cxt */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    /*
     * allocate new relation desc, clear all fields of reldesc
     */
    relation = (Relation)palloc0(sizeof(RelationData));
    /* make sure relation is marked as having no open file yet */
    relation->rd_smgr = NULL;
    relation->rd_bucketkey = NULL;
    relation->rd_bucketoid = InvalidOid;	
    relation->rd_rel = (Form_pg_class)palloc0(sizeof(FormData_pg_class));

    /*
     * initialize reference count: 1 because it is nailed in cache
     */
    relation->rd_refcnt = isnailed ? 1 : 0;
    relation->rd_isnailed = isnailed;
    relation->rd_createSubid = InvalidSubTransactionId;
    relation->rd_newRelfilenodeSubid = InvalidSubTransactionId;
    relation->rd_backend = InvalidBackendId;
    relation->rd_islocaltemp = false;
    namestrcpy(&relation->rd_rel->relname, relationName);
    relation->rd_rel->relnamespace = PG_CATALOG_NAMESPACE;
    relation->rd_rel->reltype = relationReltype;
    relation->rd_rel->relisshared = isshared;
    ereport(DEBUG1, (errmsg("relation->rd_rel->relisshared %d", relation->rd_rel->relisshared)));
    if (isshared)
        relation->rd_rel->reltablespace = GLOBALTABLESPACE_OID;
    /* formrdesc is used only for permanent relations */
    relation->rd_rel->relpersistence = RELPERSISTENCE_PERMANENT;
    relation->rd_rel->relpages = 0;
    relation->rd_rel->reltuples = 0;
    relation->rd_rel->relallvisible = 0;
    relation->rd_rel->relkind = RELKIND_RELATION;
    relation->rd_rel->relhasoids = hasoids;
    relation->rd_rel->relnatts = (int16)natts;
    // Catalog tables are heap table type.
    relation->rd_tam_type = TAM_HEAP;
    relation->rd_att = CreateTemplateTupleDesc(natts, hasoids, TAM_HEAP);
    relation->rd_att->tdrefcount = 1; /* mark as refcounted */
    relation->rd_att->tdtypeid = relationReltype;
    relation->rd_att->tdtypmod = -1;
    has_not_null = false;
    for (i = 0; i < natts; i++) {
        errno_t rc = EOK;
        rc = memcpy_s(relation->rd_att->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, &attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");
        has_not_null = has_not_null || attrs[i].attnotnull;
        /* make sure attcacheoff is valid */
        relation->rd_att->attrs[i]->attcacheoff = -1;
    }

    /* initialize first attribute's attcacheoff, cf RelationBuildTupleDesc */
    relation->rd_att->attrs[0]->attcacheoff = 0;

    /* mark not-null status */
    if (has_not_null) {
        TupleConstr* constr = (TupleConstr*)palloc0(sizeof(TupleConstr));

        constr->has_not_null = true;
        relation->rd_att->constr = constr;
    }

    /*
     * initialize relation id from info in att array (my, this is ugly)
     */
    RelationGetRelid(relation) = relation->rd_att->attrs[0]->attrelid;

    (void)MemoryContextSwitchTo(oldcxt);

    return relation;
}

CatalogRelationBuildParam GetCatalogParam(Oid targetId)
{
    struct CatalogRelationBuildParam result;
    errno_t rc = memset_s(&result, sizeof(result), 0, sizeof(result));
    securec_check(rc, "\0", "\0");
    int flag = 1;
    int low = 0;
    int high = CATALOG_NUM - 1;
    while (low <= high) {
        int mid = (low + high) / 2;
        struct CatalogRelationBuildParam midVal = catalogBuildParam[mid];
        if (midVal.oid < targetId) {
            low = mid + 1;
        } else if (midVal.oid > targetId) {
            high = mid - 1;
        } else {
            flag = 0;
            result = midVal;
            break;
        }
    }
    if (flag == 1) {
        result.oid = 0;
    }
    return result;
}

void SetBackendId(Relation relation)
{
    switch (relation->rd_rel->relpersistence) {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
        case RELPERSISTENCE_TEMP:  // @Temp Table. Temp table here is just like unlogged table.
            relation->rd_backend = InvalidBackendId;
            break;
        case RELPERSISTENCE_GLOBAL_TEMP:  // global temp table
            relation->rd_backend = BackendIdForTempRelations;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid relpersistence: %c", relation->rd_rel->relpersistence)));
            break;
    }
}

/*
 *		RelationBuildDesc
 *
 *		Build a relation descriptor.  The caller must hold at least
 *		AccessShareLock on the target relid.
 *
 *		The new descriptor is inserted into the hash table if insertIt is true.
 *
 *		Returns NULL if no pg_class row could be found for the given relid
 *		(suggesting we are trying to access a just-deleted relation).
 *		Any other error is reported via elog.
 */
Relation RelationBuildDesc(Oid targetRelId, bool insertIt, bool buildkey)
{
    Relation relation;
    HeapTuple pg_class_tuple;
    Form_pg_class relp;
    Oid relid;

    /*
     * find the tuple in pg_class corresponding to the given relation id
     */
    pg_class_tuple = ScanPgRelation(targetRelId, true, false);

    /*
     * if no such tuple exists, return NULL
     */
    if (!HeapTupleIsValid(pg_class_tuple))
        return NULL;
    /*
     * get information from the pg_class_tuple
     */
    relid = HeapTupleGetOid(pg_class_tuple);
    relp = (Form_pg_class)GETSTRUCT(pg_class_tuple);
    Assert(relid == targetRelId);
    CatalogRelationBuildParam catalogParam = GetCatalogParam(targetRelId);
    if (catalogParam.oid != 0) {
        int natts = 0;
        catalogParam.relationReltype = relp->reltype;
        relation = CatalogRelationBuildDesc(catalogParam.relationName,
            catalogParam.relationReltype,
            catalogParam.isshared,
            catalogParam.hasoids,
            catalogParam.natts,
            catalogParam.attrs,
            catalogParam.isnailed,
            insertIt);
        /*
         * Copy tuple to relation->rd_rel. (See notes in
         * AllocateRelationDesc())
         * But pay attention to relnatts of rd_rel because we hardcoded all system catalogs,
         * relnatts in pg_class might be old and will not be updated.
         * Use need to use hardcoded info in schemapg.h to fix it.
         */
        natts = relation->rd_rel->relnatts;
        errno_t rc = EOK;
        rc = memcpy_s((char*)relation->rd_rel, CLASS_TUPLE_SIZE, (char*)relp, CLASS_TUPLE_SIZE);
        securec_check(rc, "\0", "\0");
        relation->rd_rel->relnatts = natts;
    } else {
        /*
         * allocate storage for the relation descriptor, and copy pg_class_tuple
         * to relation->rd_rel.
         */
        relation = AllocateRelationDesc(relp);

        /*
         * initialize the relation's relation id (relation->rd_id)
         */
        RelationGetRelid(relation) = relid;

        /*
         * normal relations are not nailed into the cache; nor can a pre-existing
         * relation be new.  It could be temp though.  (Actually, it could be new
         * too, but it's okay to forget that fact if forced to flush the entry.)
         */
        relation->rd_refcnt = 0;
        relation->rd_isnailed = false;
        relation->rd_createSubid = InvalidSubTransactionId;
        relation->rd_newRelfilenodeSubid = InvalidSubTransactionId;
        relation->rd_islocaltemp = false;
        SetBackendId(relation);
        if (relation->rd_rel->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
            BlockNumber relpages = 0;
            double reltuples = 0;
            BlockNumber relallvisible = 0;
            get_gtt_relstats(RelationGetRelid(relation), &relpages, &reltuples, &relallvisible, NULL);
            relation->rd_rel->relpages = (float8)relpages;
            relation->rd_rel->reltuples = (float8)reltuples;
            relation->rd_rel->relallvisible = (int4)relallvisible;
        }

        /*
         * initialize the tuple descriptor (relation->rd_att).
         */
        RelationBuildTupleDesc(relation, false);
    }

    /*
     * Fetch rules and triggers that affect this relation
     */
    if (relation->rd_rel->relhasrules)
        RelationBuildRuleLock(relation);
    else {
        relation->rd_rules = NULL;
        relation->rd_rulescxt = NULL;
    }

    if (relation->rd_rel->relhastriggers)
        RelationBuildTriggers(relation);
    else
        relation->trigdesc = NULL;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && relation->rd_id >= FirstNormalObjectId)
        RelationBuildLocator(relation);
#endif
    /*
     * if it's an index, initialize index-related information
     */
    if (OidIsValid(relation->rd_rel->relam))
        RelationInitIndexAccessInfo(relation);

    /* extract reloptions if any */
    RelationParseRelOptions(relation, pg_class_tuple);

    relation->rd_att->tdhasuids = RELATION_HAS_UIDS(relation);
    if (RELATION_HAS_UIDS(relation)) {
        BuildUidHashCache(GetMyDatabaseId(), relid);
    }

    if (RelationIsRedistributeDest(relation))
        relation->rd_att->tdisredistable = true;

    /*  get the table access method type from reloptions
     * and populate them in relation and tuple descriptor */
    relation->rd_tam_type =
        get_tableam_from_reloptions(relation->rd_options, relation->rd_rel->relkind, relation->rd_rel->relam);
    relation->rd_att->tdTableAmType = relation->rd_tam_type;

    relation->rd_indexsplit = get_indexsplit_from_reloptions(relation->rd_options, relation->rd_rel->relam);

    /* get row level security policies for this relation */
    if (RelationEnableRowSecurity(relation))
        RelationBuildRlsPolicies(relation);
    else
        relation->rd_rlsdesc = NULL;

    /*
     * if it's a partitioned table or value partitioned table(HDFS), we initialize
     * partitionmap data structure for it
     */
    if (RELATION_IS_PARTITIONED(relation) || RelationIsValuePartitioned(relation)) {
        RelationInitPartitionMap(relation);
    }

    /* fetch bucket info from pgclass tuple */
    RelationInitBucketInfo(relation, pg_class_tuple);

    /* hash bucket columns cannot be changed, so there is no need to rebuild them */
    if (buildkey) {
        RelationInitBucketKey(relation, pg_class_tuple);
    }

    relation->parentId = InvalidOid;

    /*
     * initialize the relation lock manager information
     */
    RelationInitLockInfo(relation); /* see lmgr.c */

    /*
     * initialize physical addressing information for the relation
     */
    RelationInitPhysicalAddr(relation);

    /* make sure relation is marked as having no open file yet */
    relation->rd_smgr = NULL;

    if (relation->rd_rel->relkind == RELKIND_MATVIEW &&
        heap_is_matview_init_state(relation))
        relation->rd_isscannable = false;
    else
        relation->rd_isscannable = true;

    /* Get createcsn and changecsn from pg_object */
    ObjectCSN csnInfo = {InvalidCommitSeqNo, InvalidCommitSeqNo};
    GetObjectCSN(targetRelId, relation, relation->rd_rel->relkind, &csnInfo);
    relation->rd_createcsn = csnInfo.createcsn;
    relation->rd_changecsn = csnInfo.changecsn;

    if (relation->rd_id >= FirstNormalObjectId && IS_DISASTER_RECOVER_MODE) {
        TransactionId xmin = HeapTupleGetRawXmin(pg_class_tuple);
        relation->xmin_csn = CSNLogGetDRCommitSeqNo(xmin);
    } else {
        relation->xmin_csn = InvalidCommitSeqNo;
    }
    /*
     * now we can free the memory allocated for pg_class_tuple
     */
    heap_freetuple_ext(pg_class_tuple);

    /*
     * mlog oid
     */
    if (!IsCatalogRelation(relation)) {
        relation->rd_mlogoid = find_matview_mlog_table(relid);
    }

    /*
     * Check if rel is in blockchain schema.
     * Only regular table, partition table, replication table, hash table, hash bucket, list table allowed.
     * Not allowed column orientation, ORC orientation, timeseries, unlog table, temp table etc.
     */
    if (relation->rd_id >= FirstNormalObjectId && relation->rd_rel->relkind == RELKIND_RELATION &&
        IsLedgerNameSpace(relation->rd_rel->relnamespace) && !RelationIsUstoreFormat(relation) &&
        relation->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT && RelationIsRowFormat(relation)) {
        relation->rd_isblockchain = true;
    } else {
        relation->rd_isblockchain = false;
    }

    /* It's fully valid */
    relation->rd_isvalid = true;
    /*
     * Insert newly created relation into relcache hash table, if requested.
     */
    if (insertIt) {
        RelationIdCacheInsertIntoLocal(relation);
    }

    return relation;
}

/*
 * @@GaussDB@@ 
 * Brief        :
 * Description  : When load RelationData to relcache, intalize Bucket info which
 *                stores bucket oid.
 */
static void
RelationInitBucketInfo(Relation relation, HeapTuple tuple)
{
    bool  isNull = false;
    Datum datum;

    /* fetch relbucketoid from pg_class tuple */
    datum = heap_getattr(tuple,
                         Anum_pg_class_relbucket,
                         GetDefaultPgClassDesc(),
                         &isNull);
    if (isNull) {
        relation->rd_bucketoid = InvalidOid;
        relation->storage_type = HEAP_DISK;
    } else {
        relation->rd_bucketoid = DatumGetObjectId(datum);
        relation->storage_type = SEGMENT_PAGE;
        if (relation->rd_bucketoid == VirtualSegmentOid) {
            relation->rd_bucketoid = InvalidOid;
        }
        if (BUCKET_OID_IS_VALID(relation->rd_bucketoid) && RelationIsRelation(relation)) {
            relation->rd_bucketmapsize = searchBucketMapSizeByOid(relation->rd_bucketoid);
        } else if (RelationIsRelation(relation) && relation->rd_locator_info != NULL) {
            relation->rd_bucketmapsize = relation->rd_locator_info->buckets_cnt;
        }
    }
}

int16 *relationGetHBucketKey(HeapTuple tuple, int *nColumn)
{
    Datum       bkey_raw;
    bool        isNull = false;
    ArrayType  *bkey_columns = NULL;

    Assert(PointerIsValid(nColumn));
    /* Get the raw data which contain patition key's columns */
    bkey_raw = heap_getattr(tuple,
                            Anum_pg_class_relbucketkey,
                            GetDefaultPgClassDesc(),
                            &isNull);
    /* if the raw value of bucket key is null, then set bucketkey to NULL*/
    if (isNull) {
	*nColumn = 0;
        return NULL;
    }

    /*  convert Datum to ArrayType*/
    bkey_columns = DatumGetArrayTypeP(bkey_raw);

    /* Get number of bucket key columns from int2verctor*/
    *nColumn = ARR_DIMS(bkey_columns)[0];

    /*CHECK: the ArrayType of bucket key is valid*/
    if (ARR_NDIM(bkey_columns) != 1 ||
                            *nColumn < 0 ||
                            ARR_HASNULL(bkey_columns) ||
                            ARR_ELEMTYPE(bkey_columns) != INT2OID) {
        ereport(ERROR,
               (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
               errmsg("bucket key column's number is not a 1-D smallint array")));
    }

    /* Get int2 array of bucket key column numbers*/
    return (int16 *)ARR_DATA_PTR(bkey_columns);
}

/*
 * @@GaussDB@@ 
 * Brief        :
 * Description  : When load RelationData to relcache, intalize BucketKey which
 *                stores bucket key column number.
 * Notes        : We must note than the bucket key cannot be changed so we don't
 *                need to reload it. 
 */
static void
RelationInitBucketKey(Relation relation, HeapTuple tuple)
{
    MemoryContext  old_context = NULL;
    int2vector *bkey = NULL;
    Oid        *bkeytype = NULL;
    int16      *attNum = NULL;
    int         nColumn;
    Form_pg_attribute *rel_attrs = RelationGetDescr(relation)->attrs;

    if (!RelationIsRelation(relation) ||
        !OidIsValid(relation->rd_bucketoid)) {
        relation->rd_bucketkey = NULL;
        return;
    }

    /* Get int2 array of bucket key column numbers*/
    attNum = relationGetHBucketKey(tuple, &nColumn);

    /* if the raw value of bucket key is null, then set bucketkey to NULL*/
    if (attNum == NULL) {
        relation->rd_bucketkey = NULL;
        return;
    }

    /* build Bucket key */
    old_context = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /* Initialize int2verctor structure for attribute number array of bucket key*/
    bkey = buildint2vector(NULL, nColumn);
    bkeytype = (Oid*)palloc0(sizeof(Oid)*nColumn);

    /* specify value to int2verctor and build type oid array*/
    for (int i = 0; i < nColumn; i++) {
        bkey->values[i] = attNum[i];
        for (int j = 0; j < RelationGetDescr(relation)->natts; j++) {
            if (attNum[i] == rel_attrs[j]->attnum) {
                bkeytype[i] = rel_attrs[j]->atttypid;
                break;
            }
        }
    }
    relation->rd_bucketkey = (RelationBucketKey*) palloc0(sizeof(RelationBucketKey));
    relation->rd_bucketkey->bucketKey = bkey;
    relation->rd_bucketkey->bucketKeyType = bkeytype;

    (void)MemoryContextSwitchTo(old_context);
}


/*
 * Initialize the physical addressing info (RelFileNode) for a relcache entry
 *
 * Note: at the physical level, relations in the pg_global tablespace must
 * be treated as shared, even if relisshared isn't set.  Hence we do not
 * look at relisshared here.
 */
void RelationInitPhysicalAddr(Relation relation)
{
    relation->rd_node.spcNode = ConvertToRelfilenodeTblspcOid(relation->rd_rel->reltablespace);
    if (relation->rd_node.spcNode == GLOBALTABLESPACE_OID)
        relation->rd_node.dbNode = InvalidOid;
    else {
        Assert(CheckMyDatabaseMatch());
        relation->rd_node.dbNode = GetMyDatabaseId();
    }
        
    if (relation->rd_rel->relfilenode) {
        /*
         * Even if we are using a decoding snapshot that doesn't represent
         * the current state of the catalog we need to make sure the
         * filenode points to the current file since the older file will
         * be gone (or truncated). The new file will still contain older
         * rows so lookups in them will work correctly. This wouldn't work
         * correctly if rewrites were allowed to change the schema in a
         * noncompatible way, but those are prevented both on catalog
         * tables and on user tables declared as additional catalog
         * tables.
         */
        if (HistoricSnapshotActive() && RelationIsAccessibleInLogicalDecoding(relation)) {
            HeapTuple phys_tuple;
            Form_pg_class physrel;

            phys_tuple =
                ScanPgRelation(RelationGetRelid(relation), RelationGetRelid(relation) != ClassOidIndexId, true);
            if (!HeapTupleIsValid(phys_tuple))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not find pg_class entry for %u", RelationGetRelid(relation))));
            physrel = (Form_pg_class)GETSTRUCT(phys_tuple);

            relation->rd_rel->reltablespace = physrel->reltablespace;
            relation->rd_rel->relfilenode = physrel->relfilenode;
            heap_freetuple_ext(phys_tuple);
        }

        if (RELATION_IS_GLOBAL_TEMP(relation)) {
            Oid newrelnode = gtt_fetch_current_relfilenode(RelationGetRelid(relation));
            if (newrelnode != InvalidOid && newrelnode != relation->rd_rel->relfilenode) {
                relation->rd_node.relNode = newrelnode;
            } else {
                relation->rd_node.relNode = relation->rd_rel->relfilenode;
            }
        } else {
            relation->rd_node.relNode = relation->rd_rel->relfilenode;
        }

    } else {
        /* Consult the relation mapper */
        relation->rd_node.relNode = RelationMapOidToFilenode(relation->rd_id, relation->rd_rel->relisshared);
        if (!OidIsValid(relation->rd_node.relNode))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not find relation mapping for relation \"%s\", OID %u",
                        RelationGetRelationName(relation),
                        relation->rd_id)));
    }

    relation->rd_node.bucketNode = InvalidBktId;
    if (!RelationIsPartitioned(relation) && relation->storage_type == SEGMENT_PAGE) {
        relation->rd_node.bucketNode = SegmentBktId;
    }

    // setup page compression options
    relation->rd_node.opt = 0;
    if (relation->rd_options && REL_SUPPORT_COMPRESSED(relation)) {
        SetupPageCompressForRelation(&relation->rd_node, &((StdRdOptions*)(relation->rd_options))->compress, RelationGetRelationName(relation));
    }
}

static void IndexRelationInitKeyNums(Relation relation)
{
    int indnkeyatts;
    bool isnull = false;

    if (heap_attisnull(relation->rd_indextuple, Anum_pg_index_indnkeyatts, NULL)) {
        /* This scenario will only occur after the upgrade */
        indnkeyatts = RelationGetNumberOfAttributes(relation);
    } else {
        Datum indkeyDatum =
            heap_getattr(relation->rd_indextuple, Anum_pg_index_indnkeyatts, GetLSCPgIndexDescriptor(), &isnull);
        Assert(!isnull);
        indnkeyatts = DatumGetInt16(indkeyDatum);
    }

    relation->rd_indnkeyatts = indnkeyatts;
}

/*
 * Initialize index-access-method support data for an index relation
 */
void RelationInitIndexAccessInfo(Relation relation, HeapTuple index_tuple)
{
    HeapTuple tuple;
    Form_pg_am aform;
    Datum indcollDatum;
    Datum indclassDatum;
    Datum indoptionDatum;
    bool isnull = false;
    oidvector* indcoll = NULL;
    oidvector* indclass = NULL;
    int2vector* indoption = NULL;
    MemoryContext indexcxt;
    MemoryContext oldcontext;
    int indnatts;
    int indnkeyatts;
    uint16 amsupport;
    errno_t rc;

    /*
     * Make a copy of the pg_index entry for the index.  Since pg_index
     * contains variable-length and possibly-null fields, we have to do this
     * honestly rather than just treating it as a Form_pg_index struct.
     */
    if (index_tuple != NULL) {
        /* hack : for timeseries query, we cached index tuple ourselves */
        tuple = index_tuple;
    } else {
        tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(RelationGetRelid(relation)));
    }
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for index %u", RelationGetRelid(relation))));
    oldcontext = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation->rd_indextuple = heap_copytuple(tuple);
    relation->rd_index = (Form_pg_index)GETSTRUCT(relation->rd_indextuple);
    (void)MemoryContextSwitchTo(oldcontext);
    if (index_tuple == NULL) {
        ReleaseSysCache(tuple);
    }

    /* Just Use for partitionGetRelation */
    relation->rd_partHeapOid = InvalidOid;

    /*
     * Make a copy of the pg_am entry for the index's access method
     */
    tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(relation->rd_rel->relam));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for access method %u", relation->rd_rel->relam)));
    aform = (Form_pg_am)MemoryContextAlloc(LocalMyDBCacheMemCxt(), sizeof *aform);
    rc = memcpy_s(aform, sizeof(*aform), GETSTRUCT(tuple), sizeof(*aform));
    securec_check(rc, "\0", "\0");
    ReleaseSysCache(tuple);
    relation->rd_am = aform;

    indnatts = RelationGetNumberOfAttributes(relation);
    if (indnatts != IndexRelationGetNumberOfAttributes(relation))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("relnatts disagrees with indnatts for index %u", RelationGetRelid(relation))));

    IndexRelationInitKeyNums(relation);
    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(relation);
    amsupport = aform->amsupport;

    if (indnkeyatts > INDEX_MAX_KEYS) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("must index at most %u column", INDEX_MAX_KEYS)));
    }

    /*
     * Make the private context to hold index access info.	The reason we need
     * a context, and not just a couple of pallocs, is so that we won't leak
     * any subsidiary info attached to fmgr lookup records.
     *
     * Context parameters are set on the assumption that it'll probably not
     * contain much data.
     */
    indexcxt = AllocSetContextCreate(LocalMyDBCacheMemCxt(),
        RelationGetRelationName(relation),
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);

    relation->rd_indexcxt = indexcxt;

    /*
     * Allocate arrays to hold data. Opclasses are not used for included
     * columns, so allocate them for indnkeyatts only.
     */
    
        
    relation->rd_aminfo = (RelationAmInfo*)MemoryContextAllocZero(indexcxt, sizeof(RelationAmInfo));

    relation->rd_opfamily = (Oid*)MemoryContextAllocZero(indexcxt, indnkeyatts * sizeof(Oid));
    relation->rd_opcintype = (Oid*)MemoryContextAllocZero(indexcxt, indnkeyatts * sizeof(Oid));

    if (amsupport > 0) {
        int nsupport = indnatts * amsupport;

        relation->rd_support = (RegProcedure*)MemoryContextAllocZero(indexcxt, nsupport * sizeof(RegProcedure));
        relation->rd_supportinfo = (FmgrInfo*)MemoryContextAllocZero(indexcxt, nsupport * sizeof(FmgrInfo));
    } else {
        relation->rd_support = NULL;
        relation->rd_supportinfo = NULL;
    }

    relation->rd_indcollation = (Oid*)MemoryContextAllocZero(indexcxt, indnkeyatts * sizeof(Oid));

    relation->rd_indoption = (int16*)MemoryContextAllocZero(indexcxt, indnkeyatts * sizeof(int16));
    
    /*
     * indcollation cannot be referenced directly through the C struct,
     * because it comes after the variable-width indkey field.	Must extract
     * the datum the hard way...
     */
    indcollDatum = fastgetattr(relation->rd_indextuple, Anum_pg_index_indcollation,
        GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    indcoll = (oidvector*)DatumGetPointer(indcollDatum);
    rc = memcpy_s(relation->rd_indcollation, indnkeyatts * sizeof(Oid), indcoll->values, indnkeyatts * sizeof(Oid));
    securec_check(rc, "\0", "\0");

    /*
     * indclass cannot be referenced directly through the C struct, because it
     * comes after the variable-width indkey field.  Must extract the datum
     * the hard way...
     */
    indclassDatum = fastgetattr(relation->rd_indextuple, Anum_pg_index_indclass,
        GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    indclass = (oidvector*)DatumGetPointer(indclassDatum);

    /*
     * Fill the support procedure OID array, as well as the info about
     * opfamilies and opclass input types.	(aminfo and supportinfo are left
     * as zeroes, and are filled on-the-fly when used)
     */
    IndexSupportInitialize(relation,
        indclass, amsupport, indnkeyatts);

    /*
     * Similarly extract indoption and copy it to the cache entry
     */
    indoptionDatum = fastgetattr(relation->rd_indextuple, Anum_pg_index_indoption,
        GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    indoption = (int2vector*)DatumGetPointer(indoptionDatum);
    rc = memcpy_s(relation->rd_indoption, indnkeyatts * sizeof(int16), indoption->values, indnkeyatts * sizeof(int16));
    securec_check(rc, "\0", "\0");

    /*
     * expressions, predicate, exclusion caches will be filled later
     */
    relation->rd_indexprs = NIL;
    relation->rd_indpred = NIL;
    relation->rd_exclops = NULL;
    relation->rd_exclprocs = NULL;
    relation->rd_exclstrats = NULL;
    relation->rd_amcache = NULL;
    relation->rd_rootcache = InvalidBuffer;
}

/*
 * IndexSupportInitialize
 *		Initializes an index's cached opclass information,
 *		given the index's pg_index.indclass entry.
 *
 * Data is returned into *indexSupport, *opFamily, and *opcInType,
 * which are arrays allocated by the caller.
 *
 * The caller also passes maxSupportNumber and maxAttributeNumber, since these
 * indicate the size of the arrays it has allocated --- but in practice these
 * numbers must always match those obtainable from the system catalog entries
 * for the index and access method.
 */
static void IndexSupportInitialize(Relation relation, oidvector* indclass, StrategyNumber maxSupportNumber,
    AttrNumber maxAttributeNumber)
{
    RegProcedure* indexSupport = relation->rd_support;
    Oid* opFamily = relation->rd_opfamily;
    Oid* opcInType = relation->rd_opcintype;

    int attIndex;

    for (attIndex = 0; attIndex < maxAttributeNumber; attIndex++) {
        OpClassCacheEnt* opcentry = NULL;

        if (!OidIsValid(indclass->values[attIndex]))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bogus pg_index tuple")));

        /* look up the info for this opclass, using a cache */
        opcentry = LookupOpclassInfo(relation, indclass->values[attIndex], maxSupportNumber);

        /* copy cached data into relcache entry */
        opFamily[attIndex] = opcentry->opcfamily;
        opcInType[attIndex] = opcentry->opcintype;
        if (maxSupportNumber > 0) {
            errno_t rc = memcpy_s(&indexSupport[attIndex * maxSupportNumber],
                maxSupportNumber * sizeof(RegProcedure),
                opcentry->supportProcs,
                maxSupportNumber * sizeof(RegProcedure));
            securec_check(rc, "\0", "\0");
        }
    }
}

/*
 * LookupOpclassInfo
 *
 * This routine maintains a per-opclass cache of the information needed
 * by IndexSupportInitialize().  This is more efficient than relying on
 * the catalog cache, because we can load all the info about a particular
 * opclass in a single indexscan of pg_amproc.
 *
 * The information from pg_am about expected range of support function
 * numbers is passed in, rather than being looked up, mainly because the
 * caller will have it already.
 *
 * Note there is no provision for flushing the cache.  This is OK at the
 * moment because there is no way to ALTER any interesting properties of an
 * existing opclass --- all you can do is drop it, which will result in
 * a useless but harmless dead entry in the cache.	To support altering
 * opclass membership (not the same as opfamily membership!), we'd need to
 * be able to flush this cache as well as the contents of relcache entries
 * for indexes.
 */
static OpClassCacheEnt* LookupOpclassInfo(Relation relation, Oid operatorClassOid, StrategyNumber numSupport)
{
    OpClassCacheEnt* opcentry = NULL;
    bool found = false;
    Relation rel;
    SysScanDesc scan;
    ScanKeyData skey[3];
    HeapTuple htup;
    bool indexOK = false;

    if (u_sess->relcache_cxt.OpClassCache == NULL) {
        /* First time through: initialize the opclass cache */
        HASHCTL ctl;
        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(OpClassCacheEnt);
        ctl.hash = oid_hash;
        ctl.hcxt = u_sess->cache_mem_cxt;
        u_sess->relcache_cxt.OpClassCache =
            hash_create("Operator class cache", 64, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }
    opcentry = (OpClassCacheEnt *)hash_search(
        u_sess->relcache_cxt.OpClassCache, (void *)&operatorClassOid, HASH_ENTER, &found);

    /*
     * After opcentry->supportProcs palloc failed, but opcentry has been inserted
     * to the u_sess->relcache_cxt.OpClassCache and can not delete, so we will report PANIC.
     */
    START_CRIT_SECTION();
    if (!found) {
        /* Need to allocate memory for new entry */
        opcentry->valid = false; /* until known OK */
        opcentry->numSupport = numSupport;

        if (numSupport > 0)
            opcentry->supportProcs =
                (RegProcedure*)MemoryContextAllocZero(u_sess->cache_mem_cxt, numSupport * sizeof(RegProcedure));
        else
            opcentry->supportProcs = NULL;
    } else {
        Assert(numSupport == opcentry->numSupport);
    }
    END_CRIT_SECTION();

    /*
     * When testing for cache-flush hazards, we intentionally disable the
     * operator class cache and force reloading of the info on each call. This
     * is helpful because we want to test the case where a cache flush occurs
     * while we are loading the info, and it's very hard to provoke that if
     * this happens only once per opclass per backend.
     */
#if defined(CLOBBER_CACHE_ALWAYS)
    opcentry->valid = false;
#endif

    if (opcentry->valid)
        return opcentry;

    /*
     * Need to fill in new entry.
     *
     * To avoid infinite recursion during startup, force heap scans if we're
     * looking up info for the opclasses used by the indexes we would like to
     * reference here.
     */
    indexOK = LocalRelCacheCriticalRelcachesBuilt() ||
              (operatorClassOid != OID_BTREE_OPS_OID && operatorClassOid != INT2_BTREE_OPS_OID);

    /*
     * We have to fetch the pg_opclass row to determine its opfamily and
     * opcintype, which are needed to look up related operators and functions.
     * It'd be convenient to use the syscache here, but that probably doesn't
     * work while bootstrapping.
     */
    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(operatorClassOid));
    rel = heap_open(OperatorClassRelationId, AccessShareLock);
    scan = systable_beginscan(rel, OpclassOidIndexId, indexOK, NULL, 1, skey);

    if (HeapTupleIsValid(htup = systable_getnext(scan))) {
        Form_pg_opclass opclassform = (Form_pg_opclass)GETSTRUCT(htup);

        opcentry->opcfamily = opclassform->opcfamily;
        opcentry->opcintype = opclassform->opcintype;
    } else
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not find tuple for opclass %u", operatorClassOid)));

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    /*
     * Scan pg_amproc to obtain support procs for the opclass.	We only fetch
     * the default ones (those with lefttype = righttype = opcintype).
     */
    if (numSupport > 0) {
        ScanKeyInit(&skey[0],
            Anum_pg_amproc_amprocfamily,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(opcentry->opcfamily));
        ScanKeyInit(&skey[1],
            Anum_pg_amproc_amproclefttype,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(opcentry->opcintype));
        ScanKeyInit(&skey[2],
            Anum_pg_amproc_amprocrighttype,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(opcentry->opcintype));
        rel = heap_open(AccessMethodProcedureRelationId, AccessShareLock);
        scan = systable_beginscan(rel, AccessMethodProcedureIndexId, indexOK, NULL, 3, skey);

        while (HeapTupleIsValid(htup = systable_getnext(scan))) {
            Form_pg_amproc amprocform = (Form_pg_amproc)GETSTRUCT(htup);

            if (amprocform->amprocnum <= 0 || (StrategyNumber)amprocform->amprocnum > numSupport)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid amproc number %d for opclass %u", amprocform->amprocnum, operatorClassOid)));

            opcentry->supportProcs[amprocform->amprocnum - 1] = amprocform->amproc;
        }

        systable_endscan(scan);
        heap_close(rel, AccessShareLock);
    }

    opcentry->valid = true;
    return opcentry;
}

/*
 *		formrdesc
 *
 *		This is a special cut-down version of RelationBuildDesc(),
 *		used while initializing the relcache.
 *		The relation descriptor is built just from the supplied parameters,
 *		without actually looking at any system table entries.  We cheat
 *		quite a lot since we only need to work for a few basic system
 *		catalogs.
 *
 * formrdesc is currently used for: pg_database, pg_authid, pg_auth_members,
 * pg_class, pg_attribute, pg_proc, and pg_type
 * (see RelationCacheInitializePhase2/3).
 *
 * Note that these catalogs can't have constraints (except attnotnull),
 * default values, rules, or triggers, since we don't cope with any of that.
 * (Well, actually, this only matters for properties that need to be valid
 * during bootstrap or before RelationCacheInitializePhase3 runs, and none of
 * these properties matter then...)
 *
 * NOTE: we assume we are already switched into u_sess->cache_mem_cxt.
 */
extern void formrdesc(const char* relationName, Oid relationReltype, bool isshared, bool hasoids, int natts,
    const FormData_pg_attribute* attrs)
{
    Relation relation;
    int i;
    bool has_not_null = false;

    /*
     * allocate new relation desc, clear all fields of reldesc
     */
    relation = (Relation)palloc0(sizeof(RelationData));

    /* make sure relation is marked as having no open file yet */
    relation->rd_smgr = NULL;
    relation->rd_bucketkey = NULL;
    relation->rd_bucketoid = InvalidOid;

    /*
     * initialize reference count: 1 because it is nailed in cache
     */
    relation->rd_refcnt = 1;

    /*
     * all entries built with this routine are nailed-in-cache; none are for
     * new or temp relations.
     */
    relation->rd_isnailed = true;
    relation->rd_createSubid = InvalidSubTransactionId;
    relation->rd_newRelfilenodeSubid = InvalidSubTransactionId;
    relation->rd_backend = InvalidBackendId;
    relation->rd_islocaltemp = false;

    /*
     * initialize relation tuple form
     *
     * The data we insert here is pretty incomplete/bogus, but it'll serve to
     * get us launched.  RelationCacheInitializePhase3() will read the real
     * data from pg_class and replace what we've done here.  Note in
     * particular that relowner is left as zero; this cues
     * RelationCacheInitializePhase3 that the real data isn't there yet.
     */
    relation->rd_rel = (Form_pg_class)palloc0(sizeof(FormData_pg_class));

    namestrcpy(&relation->rd_rel->relname, relationName);
    relation->rd_rel->relnamespace = PG_CATALOG_NAMESPACE;
    relation->rd_rel->reltype = relationReltype;

    /*
     * It's important to distinguish between shared and non-shared relations,
     * even at bootstrap time, to make sure we know where they are stored.
     */
    relation->rd_rel->relisshared = isshared;
    ereport(DEBUG1, (errmsg("relation->rd_rel->relisshared %d", relation->rd_rel->relisshared)));
    if (isshared)
        relation->rd_rel->reltablespace = GLOBALTABLESPACE_OID;

    /* formrdesc is used only for permanent relations */
    relation->rd_rel->relpersistence = RELPERSISTENCE_PERMANENT;

    relation->rd_rel->relpages = 0;
    relation->rd_rel->reltuples = 0;
    relation->rd_rel->relallvisible = 0;
    relation->rd_rel->relkind = RELKIND_RELATION;
    relation->rd_rel->relhasoids = hasoids;
    relation->rd_rel->relnatts = (int16)natts;

    /*
     * initialize attribute tuple form
     *
     * Unlike the case with the relation tuple, this data had better be right
     * because it will never be replaced.  The data comes from
     * src/include/catalog/ headers via genbki.pl.
     */
    /* 
     * Below Catalog tables are heap table type.
       pg_database, pg_authid, pg_auth_members,  pg_class, pg_attribute, pg_proc, and pg_type 
    */
    relation->rd_att = CreateTemplateTupleDesc(natts, hasoids, TAM_HEAP);
    relation->rd_tam_type = TAM_HEAP;
    relation->rd_att->tdrefcount = 1; /* mark as refcounted */

    relation->rd_att->tdtypeid = relationReltype;
    relation->rd_att->tdtypmod = -1; /* unnecessary, but... */

    /*
     * initialize tuple desc info
     */
    has_not_null = false;
    for (i = 0; i < natts; i++) {
        errno_t rc = memcpy_s(relation->rd_att->attrs[i], ATTRIBUTE_FIXED_PART_SIZE,
            &attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "", "");
        has_not_null = has_not_null || attrs[i].attnotnull;
        /* make sure attcacheoff is valid */
        relation->rd_att->attrs[i]->attcacheoff = -1;
    }

    /* initialize first attribute's attcacheoff, cf RelationBuildTupleDesc */
    if (!RelationIsUstoreFormat(relation))
        relation->rd_att->attrs[0]->attcacheoff = 0;

    /* mark not-null status */
    if (has_not_null) {
        TupleConstr* constr = (TupleConstr*)palloc0(sizeof(TupleConstr));

        constr->has_not_null = true;
        relation->rd_att->constr = constr;
    }

    /*
     * initialize relation id from info in att array (my, this is ugly)
     */
    RelationGetRelid(relation) = relation->rd_att->attrs[0]->attrelid;

    /*
     * All relations made with formrdesc are mapped.  This is necessarily so
     * because there is no other way to know what filenode they currently
     * have.  In bootstrap mode, add them to the initial relation mapper data,
     * specifying that the initial filenode is the same as the OID.
     */
    relation->rd_rel->relfilenode = InvalidOid;
    if (IsBootstrapProcessingMode())
        RelationMapUpdateMap(RelationGetRelid(relation), RelationGetRelid(relation), isshared, true);

    relation->storage_type = HEAP_DISK;
    
    /*
     * initialize the relation lock manager information
     */
    RelationInitLockInfo(relation); /* see lmgr.c */

    /*
     * initialize physical addressing information for the relation
     */
    RelationInitPhysicalAddr(relation);

    relation->rd_isscannable = true;

    /*
     * initialize the rel-has-index flag, using hardwired knowledge
     */
    if (IsBootstrapProcessingMode()) {
        /* In bootstrap mode, we have no indexes */
        relation->rd_rel->relhasindex = false;
    } else {
        /* Otherwise, all the rels formrdesc is used for have indexes */
        relation->rd_rel->relhasindex = true;
    }

    /* It's fully valid */
    relation->rd_isvalid = true;

    /*
     * add new reldesc to relcache
     */
    RelationIdCacheInsertIntoLocal(relation);
    Assert(relation->rd_rel->relowner == InvalidOid);
}

/* ----------------------------------------------------------------
 *				 Relation Descriptor Lookup Interface
 * ----------------------------------------------------------------
 */

/*
 *		RelationIdGetRelation
 *
 *		Lookup a reldesc by OID; make one if not already in cache.
 *
 *		Returns NULL if no pg_class row could be found for the given relid
 *		(suggesting we are trying to access a just-deleted relation).
 *		Any other error is reported via elog.
 *
 *		NB: caller should already have at least AccessShareLock on the
 *		relation ID, else there are nasty race conditions.
 *
 *		NB: relation ref count is incremented, or set to 1 if new entry.
 *		Caller should eventually decrement count.  (Usually,
 *		that happens by calling RelationClose().)
 */
Relation RelationIdGetRelation(Oid relationId)
{
    Assert(CheckMyDatabaseMatch());
    if (EnableLocalSysCache()) {
        return t_thrd.lsc_cxt.lsc->tabdefcache.RelationIdGetRelation(relationId);
    }
    Relation rd;

    /*
     * first try to find reldesc in the cache
     */
    RelationIdCacheLookup(relationId, rd);

    if (RelationIsValid(rd)) {
        RelationIncrementReferenceCount(rd);
        /* revalidate cache entry if necessary */
        if (!rd->rd_isvalid) {
            /*
             * Indexes only have a limited number of possible schema changes,
             * and we don't want to use the full-blown procedure because it's
             * a headache for indexes that reload itself depends on.
             */
            if (RelationIsIndex(rd))
                RelationReloadIndexInfo(rd);
            else
                RelationClearRelation(rd, true);
        }

        /*
         * In some cases, after the relcache is built, the temp table's node group is dropped
         * because of cluster resizeing, so we should do checking when get the rel directly from
         * relcache.
         */
        if (rd->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
            (void)checkGroup(relationId, RELATION_IS_OTHER_TEMP(rd));

        return rd;
    }

    /*
     * no reldesc in the cache, so have RelationBuildDesc() build one and add
     * it.
     */
    rd = RelationBuildDesc(relationId, true, true);
    if (RelationIsValid(rd)) {
        RelationIncrementReferenceCount(rd);
        /* Insert TDE key to buffer cache for tde table */
        if (g_instance.attr.attr_security.enable_tde && IS_PGXC_DATANODE && RelationisEncryptEnable(rd)) {
            RelationInsertTdeInfoToCache(rd);
        }
    }

    return rd;
}

/* ----------------------------------------------------------------
 *				cache invalidation support routines
 * ----------------------------------------------------------------
 */

/*
 * RelationIncrementReferenceCount
 *		Increments relation reference count.
 *
 * Note: bootstrap mode has its own weird ideas about relation refcount
 * behavior; we ought to fix it someday, but for now, just disable
 * reference count ownership tracking in bootstrap mode.
 */
void RelationIncrementReferenceCount(Relation rel)
{
    /* The partition as a relation should be added to fakerelrefs rather than relrefs. */
    if (RelationIsPartition(rel) || RelationIsBucket(rel)) {
        return;
    }
    
    ResourceOwnerEnlargeRelationRefs(LOCAL_SYSDB_RESOWNER);
    rel->rd_refcnt += 1;
    if (!IsBootstrapProcessingMode())
        ResourceOwnerRememberRelationRef(LOCAL_SYSDB_RESOWNER, rel);
}

/*
 * RelationDecrementReferenceCount
 *		Decrements relation reference count.
 */
void RelationDecrementReferenceCount(Relation rel)
{
    /* The partition as a relation should be added to fakerelrefs rather than relrefs. */
    if (RelationIsPartition(rel) || RelationIsBucket(rel)) {
        return;
    }
    Assert(rel->rd_refcnt > 0);
    rel->rd_refcnt -= 1;
    if (!IsBootstrapProcessingMode())
        ResourceOwnerForgetRelationRef(LOCAL_SYSDB_RESOWNER, rel);
}

void RelationIncrementReferenceCount(Oid relationId) 
{
    RelationIdGetRelation(relationId);
}

void RelationDecrementReferenceCount(Oid relationId)
{
    Relation rd;
    RelationIdCacheLookupOnlyLocal(relationId, rd);
    if (RelationIsValid(rd)) {
        RelationDecrementReferenceCount(rd);
    }
}

/*
 * RelationClose - close an open relation
 *
 *	Actually, we just decrement the refcount.
 *
 *	NOTE: if compiled with -DRELCACHE_FORCE_RELEASE then relcache entries
 *	will be freed as soon as their refcount goes to zero.  In combination
 *	with aset.c's CLOBBER_FREED_MEMORY option, this provides a good test
 *	to catch references to already-released relcache entries.  It slows
 *	things down quite a bit, however.
 */
void RelationClose(Relation relation)
{
    /* Note: no locking manipulations needed */
    RelationDecrementReferenceCount(relation);

#ifdef RELCACHE_FORCE_RELEASE
    if (RelationHasReferenceCountZero(relation) && relation->rd_createSubid == InvalidSubTransactionId &&
        relation->rd_newRelfilenodeSubid == InvalidSubTransactionId)
        RelationClearRelation(relation, false);
#endif
}

/*
 * RelationReloadIndexInfo - reload minimal information for an open index
 *
 *	This function is used only for indexes.  A relcache inval on an index
 *	can mean that its pg_class or pg_index row changed.  There are only
 *	very limited changes that are allowed to an existing index's schema,
 *	so we can update the relcache entry without a complete rebuild; which
 *	is fortunate because we can't rebuild an index entry that is "nailed"
 *	and/or in active use.  We support full replacement of the pg_class row,
 *	as well as updates of a few simple fields of the pg_index row.
 *
 *	We can't necessarily reread the catalog rows right away; we might be
 *	in a failed transaction when we receive the SI notification.  If so,
 *	RelationClearRelation just marks the entry as invalid by setting
 *	rd_isvalid to false.  This routine is called to fix the entry when it
 *	is next needed.
 *
 *	We assume that at the time we are called, we have at least AccessShareLock
 *	on the target index.  (Note: in the calls from RelationClearRelation,
 *	this is legitimate because we know the rel has positive refcount.)
 *
 *	If the target index is an index on pg_class or pg_index, we'd better have
 *	previously gotten at least AccessShareLock on its underlying catalog,
 *	else we are at risk of deadlock against someone trying to exclusive-lock
 *	the heap and index in that order.  This is ensured in current usage by
 *	only applying this to indexes being opened or having positive refcount.
 */
void RelationReloadIndexInfo(Relation relation)
{
    bool indexOK = false;
    HeapTuple pg_class_tuple;
    Form_pg_class relp;

    /* Should be called only for invalidated indexes */
    Assert(RelationIsIndex(relation) && !relation->rd_isvalid);
    /* Should be closed at smgr level */
    Assert(relation->rd_smgr == NULL);

    /* Must free any AM cached data upon relcache flush */
    if (relation->rd_amcache)
        pfree_ext(relation->rd_amcache);
    relation->rd_amcache = NULL;
    relation->rd_rootcache = InvalidBuffer;

    /*
     * If it's a shared index, we might be called before backend startup has
     * finished selecting a database, in which case we have no way to read
     * pg_class yet.  However, a shared index can never have any significant
     * schema updates, so it's okay to ignore the invalidation signal.  Just
     * mark it valid and return without doing anything more.
     */

    ereport(DEBUG1,
        (errmsg("relation->rd_rel->relisshared-%d  criticalRelcachesBuilt-%d",
            relation->rd_rel->relisshared,
            LocalRelCacheCriticalRelcachesBuilt())));

    if (relation->rd_rel->relisshared && !LocalRelCacheCriticalRelcachesBuilt()) {
        relation->rd_isvalid = true;
        return;
    }

    /*
     * Read the pg_class row
     *
     * Don't try to use an indexscan of pg_class_oid_index to reload the info
     * for pg_class_oid_index ...
     */
    indexOK = (RelationGetRelid(relation) != ClassOidIndexId);
    pg_class_tuple = ScanPgRelation(RelationGetRelid(relation), indexOK, false);
    if (!HeapTupleIsValid(pg_class_tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not find pg_class tuple for index %u", RelationGetRelid(relation))));
    }
    relp = (Form_pg_class)GETSTRUCT(pg_class_tuple);
    errno_t rc = memcpy_s(relation->rd_rel, CLASS_TUPLE_SIZE, relp, CLASS_TUPLE_SIZE);
    securec_check(rc, "", "");
    /* Reload reloptions in case they changed */
    if (relation->rd_options)
        pfree_ext(relation->rd_options);
    RelationParseRelOptions(relation, pg_class_tuple);
    /* fetch bucket info from pgclass tuple */
    RelationInitBucketInfo(relation, pg_class_tuple);

    /* We must recalculate physical address in case it changed */
    RelationInitPhysicalAddr(relation);
    relation->rd_isscannable = true;
    /* done with pg_class tuple */
    heap_freetuple_ext(pg_class_tuple);

    /*
     * For a non-system index, there are fields of the pg_index row that are
     * allowed to change, so re-read that row and update the relcache entry.
     * Most of the info derived from pg_index (such as support function lookup
     * info) cannot change, and indeed the whole point of this routine is to
     * update the relcache entry without clobbering that data; so wholesale
     * replacement is not appropriate.
     */
    if (!IsSystemRelation(relation)) {
        HeapTuple tuple;
        Form_pg_index index;

        tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(RelationGetRelid(relation)));
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for index %u", RelationGetRelid(relation))));
        index = (Form_pg_index)GETSTRUCT(tuple);

        /*
         * Basically, let's just copy all the bool fields.  There are one or
         * two of these that can't actually change in the current code, but
         * it's not worth it to track exactly which ones they are.  None of
         * the array fields are allowed to change, though.
         */
        relation->rd_index->indisunique = index->indisunique;
        relation->rd_index->indisprimary = index->indisprimary;
        relation->rd_index->indisexclusion = index->indisexclusion;
        relation->rd_index->indimmediate = index->indimmediate;
        relation->rd_index->indisclustered = index->indisclustered;
        relation->rd_index->indisvalid = index->indisvalid;
        relation->rd_index->indcheckxmin = index->indcheckxmin;
        relation->rd_index->indisready = index->indisready;

        /* Copy xmin too, as that is needed to make sense of indcheckxmin */
        HeapTupleCopyBase(relation->rd_indextuple, tuple);
        HeapTupleSetXmin(relation->rd_indextuple, HeapTupleGetRawXmin(tuple));

        ReleaseSysCache(tuple);

        gtt_fix_index_state(relation);
    }

    /* Okay, now it's valid again */
    relation->rd_isvalid = true;
}

static void RelationDestroySliceMap(Relation relation)
{
    RangePartitionMap* range_map = (RangePartitionMap*)(relation->sliceMap);

    pfree_ext(range_map->partitionKey);
    pfree_ext(range_map->partitionKeyDataType);
    pfree_ext(range_map->intervalValue);
    pfree_ext(range_map->intervalTablespace);
    partitionMapDestroyRangeArray(range_map->rangeElements, range_map->rangeElementsNum);

    pfree_ext(relation->sliceMap);
    return;
}


static void RelationDestroyIndex(Relation rel)
{
    if (rel->rd_indexcxt != NULL) {
        MemoryContextDelete(rel->rd_indexcxt);
        rel->rd_indexcxt = NULL;
    }
}

void RelationDestroyRule(Relation rel)
{
    if (rel->rd_rulescxt != NULL) {
        MemoryContextDelete(rel->rd_rulescxt);
        rel->rd_rulescxt = NULL;
    }
}

void RelationDestroyRls(Relation rel)
{
    if (rel->rd_rlsdesc != NULL) {
        MemoryContextDelete(rel->rd_rlsdesc->rlsCxt);
        rel->rd_rlsdesc = NULL;
    }
}
/*
 * RelationDestroyRelation
 *
 *	Physically delete a relation cache entry and all subsidiary data.
 *	Caller must already have unhooked the entry from the hash table.
 */
void RelationDestroyRelation(Relation relation, bool remember_tupdesc)
{
    Assert(RelationHasReferenceCountZero(relation));

    /*
     * Make sure smgr and lower levels close the relation's files, if they
     * weren't closed already.  (This was probably done by caller, but let's
     * just be real sure.)
     */
    RelationCloseSmgr(relation);

    /*
     * Free all the subsidiary data structures of the relcache entry, then the
     * entry itself.
     */
    pfree_ext(relation->rd_rel);
    /* can't use DecrTupleDescRefCount here */
    Assert(relation->rd_att->tdrefcount > 0);
    if (--relation->rd_att->tdrefcount == 0) {
        /*
         * If we Rebuilt a relcache entry during a transaction then its
         * possible we did that because the TupDesc changed as the result
         * of an ALTER TABLE that ran at less than AccessExclusiveLock.
         * It's possible someone copied that TupDesc, in which case the
         * copy would point to free'd memory. So if we rebuild an entry
         * we keep the TupDesc around until end of transaction, to be safe.
         */
        if (remember_tupdesc) {
            RememberToFreeTupleDescAtEOX(relation->rd_att);
        } else {
            FreeTupleDesc(relation->rd_att);
        }
    }
    list_free_ext(relation->rd_indexlist);
    bms_free_ext(relation->rd_indexattr);
    bms_free_ext(relation->rd_keyattr);
    bms_free_ext(relation->rd_pkattr);
    bms_free_ext(relation->rd_idattr);
    pfree_ext(relation->rd_pubactions);
    FreeTriggerDesc(relation->trigdesc);
    RelationDestroyRls(relation);
    pfree_ext(relation->rd_options);
    pfree_ext(relation->rd_indextuple);
    pfree_ext(relation->rd_am);
    RelationDestroyIndex(relation);
    RelationDestroyRule(relation);
    pfree_ext(relation->rd_fdwroutine);
    if (relation->partMap) {
        RelationDestroyPartitionMap(relation->partMap);
    }
    if (REALTION_BUCKETKEY_VALID(relation)) {
        pfree_ext(relation->rd_bucketkey->bucketKey);
        pfree_ext(relation->rd_bucketkey->bucketKeyType);
        pfree_ext(relation->rd_bucketkey);
    }

    if (relation->rd_locator_info) {
        FreeRelationLocInfo(relation->rd_locator_info);
    }

    if (relation->sliceMap != NULL) {
        RelationDestroySliceMap(relation);
    }

    pfree_ext(relation);
}

/*
 * RelationClearRelation
 *
 *	 Physically blow away a relation cache entry, or reset it and rebuild
 *	 it from scratch (that is, from catalog entries).  The latter path is
 *	 used when we are notified of a change to an open relation (one with
 *	 refcount > 0).
 *
 *	 NB: when rebuilding, we'd better hold some lock on the relation,
 *	 else the catalog data we need to read could be changing under us.
 *	 Also, a rel to be rebuilt had better have refcnt > 0.	This is because
 *	 an sinval reset could happen while we're accessing the catalogs, and
 *	 the rel would get blown away underneath us by RelationCacheInvalidate
 *	 if it has zero refcnt.
 *
 *	 The "rebuild" parameter is redundant in current usage because it has
 *	 to match the relation's refcnt status, but we keep it as a crosscheck
 *	 that we're doing what the caller expects.
 */
void RelationClearRelation(Relation relation, bool rebuild)
{
    /*
     * As per notes above, a rel to be rebuilt MUST have refcnt > 0; while of
     * course it would be an equally bad idea to blow away one with nonzero
     * refcnt, since  that would leave someone somewhere with a dangling
     * pointer.  All callers are expected to have verified that this holds.
     */
    Assert(rebuild ? !RelationHasReferenceCountZero(relation) : RelationHasReferenceCountZero(relation));

    /*if relation is transformed from partitionGetRelation(), it is freed by releaseDummyRelation(),
     * not by RelationClearRelation(), so we can add a assertion here
     */
    Assert(!RelationIsPartition(relation));

    /*
     * Make sure smgr and lower levels close the relation's files, if they
     * weren't closed already.  If the relation is not getting deleted, the
     * next smgr access should reopen the files automatically.	This ensures
     * that the low-level file access state is updated after, say, a vacuum
     * truncation.
     */
    RelationCloseSmgr(relation);

    /*
     * Never, never ever blow away a nailed-in system relation, because we'd
     * be unable to recover.  However, we must redo RelationInitPhysicalAddr
     * in case it is a mapped relation whose mapping changed.
     *
     * If it's a nailed index, then we need to re-read the pg_class row to see
     * if its relfilenode changed.	We can't necessarily do that here, because
     * we might be in a failed transaction.  We assume it's okay to do it if
     * there are open references to the relcache entry (cf notes for
     * AtEOXact_RelationCache).  Otherwise just mark the entry as possibly
     * invalid, and it'll be fixed when next opened.
     */
    if (relation->rd_isnailed) {
        RelationInitPhysicalAddr(relation);

        if (relation->rd_rel->relkind == RELKIND_MATVIEW &&
            heap_is_matview_init_state(relation))
            relation->rd_isscannable = false;
        else
            relation->rd_isscannable = true;

        if (RelationIsIndex(relation)) {
            relation->rd_isvalid = false; /* needs to be revalidated */
            if (relation->rd_refcnt > 1)
                RelationReloadIndexInfo(relation);
        }
        list_free_ext(relation->rd_indexlist);
        relation->rd_indexlist = NIL;
        relation->rd_oidindex = InvalidOid;
        relation->rd_indexvalid = 0;
        return;
    }

    /*
     * Even non-system indexes should not be blown away if they are open and
     * have valid index support information.  This avoids problems with active
     * use of the index support information.  As with nailed indexes, we
     * re-read the pg_class row to handle possible physical relocation of the
     * index, and we check for pg_index updates too.
     */
    if (RelationIsIndex(relation) && relation->rd_refcnt > 0 && relation->rd_indexcxt != NULL) {
        relation->rd_isvalid = false; /* needs to be revalidated */
        RelationReloadIndexInfo(relation);
        return;
    }

    if (IsInitProcessingMode() && relation->rd_att->constr != NULL && relation->rd_att->constr->num_defval != 0 &&
        relation->rd_att->constr->defval == NULL)
        relation->rd_att->constr->num_defval = 0;

    /* Mark it invalid until we've finished rebuild */
    relation->rd_isvalid = false;

    /*
     * If we're really done with the relcache entry, blow it away. But if
     * someone is still using it, reconstruct the whole deal without moving
     * the physical RelationData record (so that the someone's pointer is
     * still valid).
     */
    if (!rebuild) {
        /* Remove it from the hash table */
        RelationCacheDeleteLocal(relation);

        /* And release storage */
        RelationDestroyRelation(relation, false);
    } else {
        /*
         * Our strategy for rebuilding an open relcache entry is to build a
         * new entry from scratch, swap its contents with the old entry, and
         * finally delete the new entry (along with any infrastructure swapped
         * over from the old entry).  This is to avoid trouble in case an
         * error causes us to lose control partway through.  The old entry
         * will still be marked !rd_isvalid, so we'll try to rebuild it again
         * on next access.	Meanwhile it's not any less valid than it was
         * before, so any code that might expect to continue accessing it
         * isn't hurt by the rebuild failure.  (Consider for example a
         * subtransaction that ALTERs a table and then gets canceled partway
         * through the cache entry rebuild.  The outer transaction should
         * still see the not-modified cache entry as valid.)  The worst
         * consequence of an error is leaking the necessarily-unreferenced new
         * entry, and this shouldn't happen often enough for that to be a big
         * problem.
         *
         * When rebuilding an open relcache entry, we must preserve ref count,
         * rd_createSubid/rd_newRelfilenodeSubid, and rd_toastoid state.  Also
         * attempt to preserve the pg_class entry (rd_rel), tupledesc, and
         * rewrite-rule substructures in place, because various places assume
         * that these structures won't move while they are working with an
         * open relcache entry.  (Note: the refcount mechanism for tupledescs
         * might someday allow us to remove this hack for the tupledesc.)
         *
         * Note that this process does not touch CurrentResourceOwner; which
         * is good because whatever ref counts the entry may have do not
         * necessarily belong to that resource owner.
         */
        Relation newrel = NULL;
        Oid save_relid = RelationGetRelid(relation);
        bool keep_tupdesc = false;
        bool keep_rules = false;
        bool buildkey = !REALTION_BUCKETKEY_INITED(relation);

        /* Build temporary entry, but don't link it into hashtable */
        if (EnableLocalSysCache()) {
            // call build means local doesnt contain relation or it it invalid, so search from global directly
            newrel = t_thrd.lsc_cxt.lsc->tabdefcache.SearchRelationFromGlobalCopy<false>(save_relid);
        }
        if (!RelationIsValid(newrel)) {
            newrel = RelationBuildDesc(save_relid, false, buildkey);
        }
        if (newrel == NULL) {
            /*
             * We can validly get here, if we're using a historic snapshot in
             * which a relation, accessed from outside logical decoding, is
             * still invisible. In that case it's fine to just mark the
             * relation as invalid and return - it'll fully get reloaded by
             * the cache reset at the end of logical decoding (or at the next
             * access).  During normal processing we don't want to ignore this
             * case as it shouldn't happen there, as explained below.
             */
            if (HistoricSnapshotActive())
                return;

            /*
             * This shouldn't happen as dropping a relation is intended to be
             * impossible if still referenced (c.f. CheckTableNotInUse()). But
             * if we get here anyway, we can't just delete the relcache entry,
             * as it possibly could get accessed later (as e.g. the error
             * might get trapped and handled via a subtransaction rollback).
             */
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("relation %u deleted while still in use", save_relid)));
        }

        newrel->rd_isnailed = relation->rd_isnailed;
        keep_tupdesc = equalTupleDescs(relation->rd_att, newrel->rd_att);
        keep_rules = equalRuleLocks(relation->rd_rules, newrel->rd_rules);

        /*
         * Perform swapping of the relcache entry contents.  Within this
         * process the old entry is momentarily invalid, so there *must* be no
         * possibility of CHECK_FOR_INTERRUPTS within this sequence. Do it in
         * all-in-line code for safety.
         *
         * Since the vast majority of fields should be swapped, our method is
         * to swap the whole structures and then re-swap those few fields we
         * didn't want swapped.
         */
#define SWAPFIELD(fldtype, fldname)          \
    do {                                     \
        fldtype _tmp = newrel->fldname;      \
        newrel->fldname = relation->fldname; \
        relation->fldname = _tmp;            \
    } while (0)

        /* swap all Relation struct fields */
        {
            RelationData tmpstruct;

            errno_t rc = memcpy_s(&tmpstruct, sizeof(RelationData), newrel, sizeof(RelationData));
            securec_check(rc, "", "");
            rc = memcpy_s(newrel, sizeof(RelationData), relation, sizeof(RelationData));
            securec_check(rc, "", "");
            rc = memcpy_s(relation, sizeof(RelationData), &tmpstruct, sizeof(RelationData));
            securec_check(rc, "", "");
        }

        /* rd_smgr must not be swapped, due to back-links from smgr level */
        SWAPFIELD(SMgrRelation, rd_smgr);
        /* rd_refcnt must be preserved */
        SWAPFIELD(int, rd_refcnt);
        /* isnailed shouldn't change */
        Assert(newrel->rd_isnailed == relation->rd_isnailed);
        /* creation sub-XIDs must be preserved */
        SWAPFIELD(SubTransactionId, rd_createSubid);
        SWAPFIELD(SubTransactionId, rd_newRelfilenodeSubid);
        /* un-swap rd_rel pointers, swap contents instead */
        SWAPFIELD(Form_pg_class, rd_rel);
        /* ... but actually, we don't have to update newrel->rd_rel */
        errno_t rc = memcpy_s(relation->rd_rel, CLASS_TUPLE_SIZE, newrel->rd_rel, CLASS_TUPLE_SIZE);
        securec_check(rc, "", "");
        if (newrel->partMap) {
            RebuildPartitonMap(newrel->partMap, relation->partMap);
            SWAPFIELD(PartitionMap*, partMap);
        }

        if (!buildkey) {
            /* no need to rebuild bucket key info, so just preserve it */
            SWAPFIELD(RelationBucketKey*, rd_bucketkey);
        } else {
            /* no need to free rd_bucketkey from relation */
            newrel->rd_bucketkey = NULL;
        }
        /* preserve old tupledesc and rules if no logical change */
        if (keep_tupdesc)
            SWAPFIELD(TupleDesc, rd_att);
        if (keep_rules) {
            SWAPFIELD(RuleLock*, rd_rules);
            SWAPFIELD(MemoryContext, rd_rulescxt);
        }
        /* toast OID override must be preserved */
        SWAPFIELD(Oid, rd_toastoid);
        /* pgstat_info must be preserved */
        SWAPFIELD(struct PgStat_TableStatus*, pgstat_info);

        /* newcbi flag and its related information must be preserved */
        if (newrel->newcbi) {
            SWAPFIELD(bool, newcbi);
            relation->rd_node.bucketNode = newrel->rd_node.bucketNode;
        }
        SWAPFIELD(LocalRelationEntry*, entry);
#undef SWAPFIELD

        /* And now we can throw away the temporary entry */
        RelationDestroyRelation(newrel, !keep_tupdesc);
    }
}

/*
 * RelationFlushRelation
 *
 *	 Rebuild the relation if it is open (refcount > 0), else blow it away.
 */
static void RelationFlushRelation(Relation relation)
{
    if (relation->rd_createSubid != InvalidSubTransactionId ||
        relation->rd_newRelfilenodeSubid != InvalidSubTransactionId) {
        /*
         * New relcache entries are always rebuilt, not flushed; else we'd
         * forget the "new" status of the relation, which is a useful
         * optimization to have.  Ditto for the new-relfilenode status.
         *
         * The rel could have zero refcnt here, so temporarily increment the
         * refcnt to ensure it's safe to rebuild it.  We can assume that the
         * current transaction has some lock on the rel already.
         */
        RelationIncrementReferenceCount(relation);
        RelationClearRelation(relation, true);
        RelationDecrementReferenceCount(relation);
    } else {
        /*
         * Pre-existing rels can be dropped from the relcache if not open.
         */
        bool rebuild = !RelationHasReferenceCountZero(relation);

        RelationClearRelation(relation, rebuild);
    }
}

/*
 * RelationForgetRelation - unconditionally remove a relcache entry
 *
 *		   External interface for destroying a relcache entry when we
 *		   drop the relation.
 */
void RelationForgetRelation(Oid rid)
{
    Relation relation;

    RelationIdCacheLookupOnlyLocal(rid, relation);

    if (!PointerIsValid(relation))
        return; /* not in cache, nothing to do */

    if (!RelationHasReferenceCountZero(relation))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("relation %u is still open", rid)));

    /* Unconditionally destroy the relcache entry */
    RelationClearRelation(relation, false);
}

/*
 *		RelationCacheInvalidateEntry
 *
 *		This routine is invoked for SI cache flush messages.
 *
 * Any relcache entry matching the relid must be flushed.  (Note: caller has
 * already determined that the relid belongs to our database or is a shared
 * relation.)
 *
 * We used to skip local relations, on the grounds that they could
 * not be targets of cross-backend SI update messages; but it seems
 * safer to process them, so that our *own* SI update messages will
 * have the same effects during CommandCounterIncrement for both
 * local and nonlocal relations.
 */
void RelationCacheInvalidateEntry(Oid relationId)
{
    if (unlikely(relationId == InvalidOid)) {
        RelationCacheInvalidate();
        return;
    }
    Relation relation;

    RelationIdCacheLookupOnlyLocal(relationId, relation);

    if (PointerIsValid(relation)) {
        AddLocalRelCacheInvalsReceived(1);
        RelationFlushRelation(relation);
    }
}

/*
 * RelationCacheInvalidate
 *	 Blow away cached relation descriptors that have zero reference counts,
 *	 and rebuild those with positive reference counts.	Also reset the smgr
 *	 relation cache and re-read relation mapping data.
 *
 *	 This is currently used only to recover from SI message buffer overflow,
 *	 so we do not touch new-in-transaction relations; they cannot be targets
 *	 of cross-backend SI updates (and our own updates now go through a
 *	 separate linked list that isn't limited by the SI message buffer size).
 *	 Likewise, we need not discard new-relfilenode-in-transaction hints,
 *	 since any invalidation of those would be a local event.
 *
 *	 We do this in two phases: the first pass deletes deletable items, and
 *	 the second one rebuilds the rebuildable items.  This is essential for
 *	 safety, because hash_seq_search only copes with concurrent deletion of
 *	 the element it is currently visiting.	If a second SI overflow were to
 *	 occur while we are walking the table, resulting in recursive entry to
 *	 this routine, we could crash because the inner invocation blows away
 *	 the entry next to be visited by the outer scan.  But this way is OK,
 *	 because (a) during the first pass we won't process any more SI messages,
 *	 so hash_seq_search will complete safely; (b) during the second pass we
 *	 only hold onto pointers to nondeletable entries.
 *
 *	 The two-phase approach also makes it easy to update relfilenodes for
 *	 mapped relations before we do anything else, and to ensure that the
 *	 second pass processes nailed-in-cache items before other nondeletable
 *	 items.  This should ensure that system catalogs are up to date before
 *	 we attempt to use them to reload information about other open relations.
 */

void RelationCacheInvalidate(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.InvalidateRelationAll();
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;
    Relation relation;
    List* rebuildFirstList = NIL;
    List* rebuildList = NIL;
    ListCell* l = NULL;

    /*
     * Reload relation mapping data before starting to reconstruct cache.
     */
    RelationMapInvalidateAll();

    /* Phase 1 */
    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        relation = idhentry->reldesc;

        /* Must close all smgr references to avoid leaving dangling ptrs */
        RelationCloseSmgr(relation);

        /* Ignore new relations, since they are never cross-backend targets */
        if (relation->rd_createSubid != InvalidSubTransactionId)
            continue;

        u_sess->relcache_cxt.relcacheInvalsReceived++;

        if (RelationHasReferenceCountZero(relation)) {
            /* Delete this entry immediately */
            Assert(!relation->rd_isnailed);
            RelationClearRelation(relation, false);
        } else {
            /*
             * If it's a mapped relation, immediately update its rd_node in
             * case its relfilenode changed.  We must do this during phase 1
             * in case the relation is consulted during rebuild of other
             * relcache entries in phase 2.  It's safe since consulting the
             * map doesn't involve any access to relcache entries.
             */
            if (RelationIsMapped(relation))
                RelationInitPhysicalAddr(relation);

            /*
             * Add this entry to list of stuff to rebuild in second pass.
             * pg_class goes to the front of rebuildFirstList while
             * pg_class_oid_index goes to the back of rebuildFirstList, so
             * they are done first and second respectively.  Other nailed
             * relations go to the front of rebuildList, so they'll be done
             * next in no particular order; and everything else goes to the
             * back of rebuildList.
             */
            if (RelationGetRelid(relation) == RelationRelationId)
                rebuildFirstList = lcons(relation, rebuildFirstList);
            else if (RelationGetRelid(relation) == ClassOidIndexId)
                rebuildFirstList = lappend(rebuildFirstList, relation);
            else if (relation->rd_isnailed)
                rebuildList = lcons(relation, rebuildList);
            else
                rebuildList = lappend(rebuildList, relation);
        }
    }

    /*
     * Now zap any remaining smgr cache entries.  This must happen before we
     * start to rebuild entries, since that may involve catalog fetches which
     * will re-open catalog files.
     */
    smgrcloseall();

    /* Phase 2: rebuild the items found to need rebuild in phase 1 */
    foreach (l, rebuildFirstList) {
        relation = (Relation)lfirst(l);
        RelationClearRelation(relation, true);
    }
    list_free_ext(rebuildFirstList);
    foreach (l, rebuildList) {
        relation = (Relation)lfirst(l);
        RelationClearRelation(relation, true);
    }
    list_free_ext(rebuildList);
}

/*
 * RelationCacheInvalidateBuckets
 *     Invalidate bucket_ptr in all relcache entries.
 *
 * Need to invalidate bucket_ptr  after modify node group.
 */

void RelationCacheInvalidateBuckets(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.InvalidateRelationBucketsAll();
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;
    Relation relation;

    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        relation = idhentry->reldesc;
        if (relation->rd_locator_info != NULL) {
            InvalidateBuckets(relation->rd_locator_info);
        }
    }
}

void InvalidateRelationNodeList()
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.InvalidateRelationNodeList();
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;
    Relation relation;

    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        relation = idhentry->reldesc;
        if (relation->rd_locator_info != NULL) {
            RelationClearRelation(relation, !RelationHasReferenceCountZero(relation));
        }
    }
}

/*
 * RelationCloseSmgrByOid - close a relcache entry's smgr link
 *
 * Needed in some cases where we are changing a relation's physical mapping.
 * The link will be automatically reopened on next use.
 */
void RelationCloseSmgrByOid(Oid relationId)
{
    Relation relation;

    RelationIdCacheLookupOnlyLocal(relationId, relation);

    if (!PointerIsValid(relation))
        return; /* not in cache, nothing to do */

    RelationCloseSmgr(relation);
}

TransactionId RelationGetRelFrozenxid64(Relation r)
{
    Relation classRel;
    HeapTuple tuple;
    Datum datum;
    bool isNull;
    TransactionId relfrozenxid64;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(r)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(r))));
    }

    classRel = heap_open(RelationRelationId, AccessShareLock);

    datum = heap_getattr(tuple, Anum_pg_class_relfrozenxid64, RelationGetDescr(classRel), &isNull);
    if (isNull) {
        relfrozenxid64 = r->rd_rel->relfrozenxid;
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid64) ||
            !TransactionIdIsNormal(relfrozenxid64))
            relfrozenxid64 = FirstNormalTransactionId;
    } else {
        relfrozenxid64 = DatumGetTransactionId(datum);
    }
    
    heap_close(classRel, AccessShareLock);
    ReleaseSysCache(tuple);

    return relfrozenxid64;
}

TransactionId PartGetRelFrozenxid64(Partition part)
{
    Relation partRel;
    HeapTuple partTuple;
    Datum datum;
    bool isNull;
    TransactionId relfrozenxid64;

    partTuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part->pd_id));
    if (!HeapTupleIsValid(partTuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("cache lookup failed for relation %u", part->pd_id)));
    }

    partRel = heap_open(PartitionRelationId, AccessShareLock);

    datum = heap_getattr(partTuple, Anum_pg_partition_relfrozenxid64, RelationGetDescr(partRel), &isNull);
    if (isNull) {
        relfrozenxid64 = part->pd_part->relfrozenxid;
        if (TransactionIdPrecedes(t_thrd.xact_cxt.ShmemVariableCache->nextXid, relfrozenxid64) ||
            !TransactionIdIsNormal(relfrozenxid64))
            relfrozenxid64 = FirstNormalTransactionId;
    } else {
        relfrozenxid64 = DatumGetTransactionId(datum);
    }
    
    heap_close(partRel, AccessShareLock);
    ReleaseSysCache(partTuple);

    return relfrozenxid64;
}

Oid RelationGetBucketOid(Relation relation)
{
    return relation->rd_bucketoid;
}

/* Remember old tupleDescs when processing invalid messages */

void RememberToFreeTupleDescAtEOX(TupleDesc td)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.RememberToFreeTupleDescAtEOX(td);
        return;
    }
    if (u_sess->relcache_cxt.EOXactTupleDescArray == NULL) {
        MemoryContext	oldcxt = NULL;
        oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

        u_sess->relcache_cxt.EOXactTupleDescArray = (TupleDesc *) palloc(16 * sizeof(TupleDesc));
        u_sess->relcache_cxt.EOXactTupleDescArrayLen = 16;
        u_sess->relcache_cxt.NextEOXactTupleDescNum = 0;
        MemoryContextSwitchTo(oldcxt);
    } else if (u_sess->relcache_cxt.NextEOXactTupleDescNum >= u_sess->relcache_cxt.EOXactTupleDescArrayLen) {
        int32 newlen = u_sess->relcache_cxt.EOXactTupleDescArrayLen * 2;

        Assert(u_sess->relcache_cxt.EOXactTupleDescArrayLen > 0);

        u_sess->relcache_cxt.EOXactTupleDescArray = (TupleDesc *) repalloc(u_sess->relcache_cxt.EOXactTupleDescArray,
                                                                           newlen * sizeof(TupleDesc));
        u_sess->relcache_cxt.EOXactTupleDescArrayLen = newlen;
    }

    u_sess->relcache_cxt.EOXactTupleDescArray[u_sess->relcache_cxt.NextEOXactTupleDescNum++] = td;
}

/* Free all tupleDescs remembered in RememberToFreeTupleDescAtEOX in a batch when a transaction ends */
void AtEOXact_FreeTupleDesc()
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.AtEOXact_FreeTupleDesc();
        return;
    }
    if (u_sess->relcache_cxt.EOXactTupleDescArrayLen > 0) {
        Assert(u_sess->relcache_cxt.EOXactTupleDescArray != NULL);
        int i;
        for (i = 0; i < u_sess->relcache_cxt.NextEOXactTupleDescNum; i++)
            FreeTupleDesc(u_sess->relcache_cxt.EOXactTupleDescArray[i]);
        pfree(u_sess->relcache_cxt.EOXactTupleDescArray);
        u_sess->relcache_cxt.EOXactTupleDescArray = NULL;
    }

    u_sess->relcache_cxt.NextEOXactTupleDescNum = 0;
    u_sess->relcache_cxt.EOXactTupleDescArrayLen = 0;
}

/*
 * AtEOXact_RelationCache
 *
 *	Clean up the relcache at main-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 * In the case of abort, we don't want to try to rebuild any invalidated
 * cache entries (since we can't safely do database accesses).  Therefore
 * we must reset refcnts before handling pending invalidations.
 *
 * As of PostgreSQL 8.1, relcache refcnts should get released by the
 * ResourceOwner mechanism.  This routine just does a debugging
 * cross-check that no pins remain.  However, we also need to do special
 * cleanup when the current transaction created any relations or made use
 * of forced index lists.
 */
void AtEOXact_RelationCache(bool isCommit)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.AtEOXact_RelationCache(isCommit);
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;

    /*
     * To speed up transaction exit, we want to avoid scanning the relcache
     * unless there is actually something for this routine to do.  Other than
     * the debug-only Assert checks, most transactions don't create any work
     * for us to do here, so we keep a static flag that gets set if there is
     * anything to do.	(Currently, this means either a relation is created in
     * the current xact, or one is given a new relfilenode, or an index list
     * is forced.)	For simplicity, the flag remains set till end of top-level
     * transaction, even though we could clear it at subtransaction end in
     * some cases.
     */
    if (!GetRelCacheNeedEOXActWork()
#ifdef USE_ASSERT_CHECKING
        && !assert_enabled
#endif
    )
        return;

    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Relation relation = idhentry->reldesc;

        /*
         * The relcache entry's ref count should be back to its normal
         * not-in-a-transaction state: 0 unless it's nailed in cache.
         *
         * In bootstrap mode, this is NOT true, so don't check it --- the
         * bootstrap code expects relations to stay open across start/commit
         * transaction calls.  (That seems bogus, but it's not worth fixing.)
         */
        if (!IsBootstrapProcessingMode()) {
            int expected_refcnt;

            expected_refcnt = relation->rd_isnailed ? 1 : 0;
            if (relation->rd_refcnt != expected_refcnt && IsolatedResourceOwner != NULL) {
                elog(WARNING,
                    "relation \"%s\" rd_refcnt is %d but expected_refcnt %d. ",
                    RelationGetRelationName(relation),
                    relation->rd_refcnt,
                    expected_refcnt);
                PrintResourceOwnerLeakWarning();
            }
#ifdef USE_ASSERT_CHECKING
            Assert(relation->rd_refcnt == expected_refcnt);
#endif
        }

        /*
         * Is it a relation created in the current transaction?
         *
         * During commit, reset the flag to zero, since we are now out of the
         * creating transaction.  During abort, simply delete the relcache
         * entry --- it isn't interesting any longer.  (NOTE: if we have
         * forgotten the new-ness of a new relation due to a forced cache
         * flush, the entry will get deleted anyway by shared-cache-inval
         * processing of the aborted pg_class insertion.)
         */
        if (relation->rd_createSubid != InvalidSubTransactionId) {
            if (isCommit)
                relation->rd_createSubid = InvalidSubTransactionId;
            else if (RelationHasReferenceCountZero(relation)) {
                RelationClearRelation(relation, false);
                continue;
            } else {
                /*
                 * Hmm, somewhere there's a (leaked?) reference to the relation.
                 * We daren't remove the entry for fear of dereferencing a
                 * dangling pointer later.  Bleat, and mark it as not belonging to
                 * the current transaction.  Hopefully it'll get cleaned up
                 * eventually.  This must be just a WARNING to avoid
                 * error-during-error-recovery loops.
                 */
                relation->rd_createSubid = InvalidSubTransactionId;
                ereport(WARNING,
                    (errmsg("cannot remove relcache entry for \"%s\" because it has nonzero refcount",
                        RelationGetRelationName(relation))));
            }
        }

        /*
         * Likewise, reset the hint about the relfilenode being new.
         */
        relation->rd_newRelfilenodeSubid = InvalidSubTransactionId;

        /*
         * Flush any temporary index list.
         */
        if (relation->rd_indexvalid == 2) {
            list_free_ext(relation->rd_indexlist);
            relation->rd_indexlist = NIL;
            relation->rd_oidindex = InvalidOid;
            relation->rd_pkindex = InvalidOid;
            relation->rd_indexvalid = 0;
        }
        if (relation->partMap != NULL && relation->partMap->isDirty) {
            RelationClearRelation(relation, false);
            hash_seq_term(&status);
            hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);
        }
    }

    /* Once done with the transaction, we can reset u_sess->relcache_cxt.need_eoxact_work */
    SetRelCacheNeedEOXActWork(false);
}

/*
 * AtEOSubXact_RelationCache
 *
 *	Clean up the relcache at sub-transaction commit or abort.
 *
 * Note: this must be called *before* processing invalidation messages.
 */
void AtEOSubXact_RelationCache(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.AtEOSubXact_RelationCache(isCommit, mySubid, parentSubid);
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;

    /*
     * Skip the relcache scan if nothing to do --- see notes for
     * AtEOXact_RelationCache.
     */
    if (!GetRelCacheNeedEOXActWork())
        return;

    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Relation relation = idhentry->reldesc;

        /*
         * Is it a relation created in the current subtransaction?
         *
         * During subcommit, mark it as belonging to the parent, instead.
         * During subabort, simply delete the relcache entry.
         */
        if (relation->rd_createSubid == mySubid) {
            if (isCommit)
                relation->rd_createSubid = parentSubid;
            else if (RelationHasReferenceCountZero(relation)) {
                RelationClearRelation(relation, false);
                continue;
            } else {
                /*
                 * Hmm, somewhere there's a (leaked?) reference to the relation.
                 * We daren't remove the entry for fear of dereferencing a
                 * dangling pointer later.  Bleat, and transfer it to the parent
                 * subtransaction so we can try again later.  This must be just a
                 * WARNING to avoid error-during-error-recovery loops.
                 */
                relation->rd_createSubid = parentSubid;
                ereport(WARNING,
                    (errmsg("cannot remove relcache entry for \"%s\" because it has nonzero refcount",
                        RelationGetRelationName(relation))));
            }
        }

        /*
         * Likewise, update or drop any new-relfilenode-in-subtransaction
         * hint.
         */
        if (relation->rd_newRelfilenodeSubid == mySubid) {
            if (isCommit)
                relation->rd_newRelfilenodeSubid = parentSubid;
            else
                relation->rd_newRelfilenodeSubid = InvalidSubTransactionId;
        }

        /*
         * Flush any temporary index list.
         */
        if (relation->rd_indexvalid == 2) {
            list_free_ext(relation->rd_indexlist);
            relation->rd_indexlist = NIL;
            relation->rd_oidindex = InvalidOid;
            relation->rd_pkindex = InvalidOid;
            relation->rd_indexvalid = 0;
        }
    }
}

/*
 *		RelationBuildLocalRelation
 *			Build a relcache entry for an about-to-be-created relation,
 *			and enter it into the relcache.
 */
Relation RelationBuildLocalRelation(const char* relname, Oid relnamespace, TupleDesc tupDesc, Oid relid,
    Oid relfilenode, Oid reltablespace, bool shared_relation, bool mapped_relation, char relpersistence,
    char relkind, int8 row_compress, Datum reloptions, TableAmType tam_type, int8 relindexsplit,
    StorageType storage_type, Oid accessMethodObjectId)
{
    Relation rel;
    MemoryContext oldcxt;
    int natts = tupDesc->natts;
    int i;
    bool has_not_null = false;
    bool nailit = false;

    AssertArg(natts >= 0);

    /*
     * check for creation of a rel that must be nailed in cache.
     *
     * XXX this list had better match the relations specially handled in
     * RelationCacheInitializePhase2/3.
     */
    switch (relid) {
        case DatabaseRelationId:
        case AuthIdRelationId:
        case AuthMemRelationId:
        case RelationRelationId:
        case AttributeRelationId:
        case ProcedureRelationId:
        case TypeRelationId:
        case UserStatusRelationId:
            nailit = true;
            break;
        default:
            nailit = false;
            break;
    }

    /*
     * check that hardwired list of shared rels matches what's in the
     * bootstrap .bki file.  If you get a failure here during initdb, you
     * probably need to fix IsSharedRelation() to match whatever you've done
     * to the set of shared relations.
     */
    if (shared_relation != IsSharedRelation(relid))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("shared_relation flag for \"%s\" does not match IsSharedRelation(%u)", relname, relid)));

    /* Shared relations had better be mapped, too */
    Assert(mapped_relation || !shared_relation);

    /*
     * switch to the cache context to create the relcache entry.
     */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    /*
     * allocate a new relation descriptor and fill in basic state fields.
     */
    rel = (Relation)palloc0(sizeof(RelationData));

    /* make sure relation is marked as having no open file yet */
    rel->rd_smgr = NULL;
    /* init bucketkey to point itself means bucket key is not build yet */
    rel->rd_bucketkey = (RelationBucketKey *)&(rel->rd_bucketkey);
    rel->rd_bucketoid = InvalidOid;

    /* mark it nailed if appropriate */
    rel->rd_isnailed = nailit;

    rel->rd_refcnt = nailit ? 1 : 0;

    /* it's being created in this transaction */
    rel->rd_createSubid = GetCurrentSubTransactionId();
    rel->rd_newRelfilenodeSubid = InvalidSubTransactionId;

    /* must flag that we have rels created in this transaction */
    SetRelCacheNeedEOXActWork(true);

    /*
     * create a new tuple descriptor from the one passed in.  We do this
     * partly to copy it into the cache context, and partly because the new
     * relation can't have any defaults or constraints yet; they have to be
     * added in later steps, because they require additions to multiple system
     * catalogs.  We can copy attnotnull constraints here, however.
     */
    rel->rd_att = CreateTupleDescCopy(tupDesc);
    rel->rd_tam_type = tam_type;
    rel->rd_indexsplit = relindexsplit;
    rel->rd_att->tdTableAmType = tam_type;
    rel->rd_att->tdrefcount = 1; /* mark as refcounted */
    has_not_null = false;
    for (i = 0; i < natts; i++) {
        rel->rd_att->attrs[i]->attnotnull = tupDesc->attrs[i]->attnotnull;
        has_not_null = has_not_null || tupDesc->attrs[i]->attnotnull;
    }

    if (has_not_null) {
        TupleConstr* constr = (TupleConstr*)palloc0(sizeof(TupleConstr));

        constr->has_not_null = true;
        rel->rd_att->constr = constr;
    }

    /*
     * initialize relation tuple form (caller may add/override data later)
     */
    rel->rd_rel = (Form_pg_class)palloc0(sizeof(FormData_pg_class));

    namestrcpy(&rel->rd_rel->relname, relname);
    rel->rd_rel->relnamespace = relnamespace;

    rel->rd_rel->relkind = relkind;
    rel->rd_rel->relhasoids = rel->rd_att->tdhasoid;
    rel->rd_rel->relnatts = natts;
    rel->rd_rel->reltype = InvalidOid;
    /* needed when bootstrapping: */
    rel->rd_rel->relowner = BOOTSTRAP_SUPERUSERID;
    rel->rd_rel->parttype = PARTTYPE_NON_PARTITIONED_RELATION;
    rel->rd_rel->relrowmovement = false;
    rel->rd_rel->relam = accessMethodObjectId;

    /* set up persistence and relcache fields dependent on it */
    rel->rd_rel->relpersistence = relpersistence;
    switch (relpersistence) {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
        case RELPERSISTENCE_TEMP:  //@Temp Table. Temp table here is just like unlogged table.
            rel->rd_backend = InvalidBackendId;
            rel->rd_islocaltemp = false;
            break;
        case RELPERSISTENCE_GLOBAL_TEMP:  // global temp table
            rel->rd_backend = BackendIdForTempRelations;
            rel->rd_islocaltemp = false;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relpersistence: %c", relpersistence)));
            break;
    }

    /*
     * Insert relation physical and logical identifiers (OIDs) into the right
     * places.	For a mapped relation, we set relfilenode to zero and rely on
     * RelationInitPhysicalAddr to consult the map.
     */
    rel->rd_rel->relisshared = shared_relation;

    RelationGetRelid(rel) = relid;

    for (i = 0; i < natts; i++)
        rel->rd_att->attrs[i]->attrelid = relid;

    rel->rd_rel->reltablespace = reltablespace;

    if (mapped_relation) {
        rel->rd_rel->relfilenode = InvalidOid;
        /* Add it to the active mapping information */
        RelationMapUpdateMap(relid, relfilenode, shared_relation, true);
    } else
        rel->rd_rel->relfilenode = relfilenode;

    /*
     * case 1: table in system level;
     * case 2: invalid COMPRESS type;
     */
    if ((relid < FirstNormalObjectId) || !CHECK_CMPRS_VALID(row_compress)) {
        row_compress = REL_CMPRS_NOT_SUPPORT;
    }
    RELATION_SET_CMPRS_ATTR(rel, row_compress);

    rel->storage_type = storage_type;

    RelationInitLockInfo(rel); /* see lmgr.c */

    RelationInitPhysicalAddr(rel);

    /* compressed option was set by RelationInitPhysicalAddr if rel->rd_options != NULL */
    if (rel->rd_options == NULL && reloptions && SUPPORT_COMPRESSED(relkind, rel->rd_rel->relam)) {
        StdRdOptions *options = (StdRdOptions *) default_reloptions(reloptions, false, RELOPT_KIND_HEAP);
        SetupPageCompressForRelation(&rel->rd_node, &options->compress, RelationGetRelationName(rel));
    }


    /* materialized view not initially scannable */
    if (relkind == RELKIND_MATVIEW)
        rel->rd_isscannable = false;
    else
        rel->rd_isscannable = true;

    /* It's fully valid */
    rel->rd_isvalid = true;

    /*
     * Okay to insert into the relcache hash tables.
     */
    RelationIdCacheInsertIntoLocal(rel);

    /*
     * done building relcache entry.
     */
    (void)MemoryContextSwitchTo(oldcxt);

    

    /*
     * Caller expects us to pin the returned entry.
     */
    RelationIncrementReferenceCount(rel);

    return rel;
}

// function protype declaim
extern void heap_create_init_fork(Relation rel);

// set new filenode for delta table
void DeltaTableSetNewRelfilenode(Oid relid, TransactionId freezeXid, bool partition)
{
    Relation deltaRel = heap_open(relid, AccessExclusiveLock);
    RelationSetNewRelfilenode(deltaRel, freezeXid, InvalidMultiXactId);
    // skip partition because one partition CANNOT be unlogged.
    if (!partition && RELPERSISTENCE_UNLOGGED == deltaRel->rd_rel->relpersistence) {
        heap_create_init_fork(deltaRel);
    }
    heap_close(deltaRel, NoLock);
}

// set new filenode for CUDesc table
void DescTableSetNewRelfilenode(Oid relid, TransactionId freezeXid, bool partition)
{
    // Step 1: CUDesc relation must set new relfilenode
    // Because indexRelation has locked as AccessExclusiveLock, so it is safe
    //
    Relation cudescRel = heap_open(relid, AccessExclusiveLock);
    RelationSetNewRelfilenode(cudescRel, freezeXid, InvalidMultiXactId);

    // skip partition because one partition CANNOT be unlogged.
    if (!partition && RELPERSISTENCE_UNLOGGED == cudescRel->rd_rel->relpersistence) {
        heap_create_init_fork(cudescRel);
    }

    // Step 2: CUDesc index must be set new relfilenode
    //
    ListCell* indlist = NULL;
    foreach (indlist, RelationGetIndexList(cudescRel)) {
        Oid indexId = lfirst_oid(indlist);
        Relation currentIndex = index_open(indexId, AccessExclusiveLock);
        RelationSetNewRelfilenode(currentIndex, InvalidTransactionId, InvalidMultiXactId);
        // keep the same logic with row index relation, and
        // skip checking RELPERSISTENCE_UNLOGGED persistence

        /* Initialize the index and rebuild */
        /* Note: we do not need to re-establish pkey setting */
        /* Fetch info needed for index_build */
        IndexInfo* indexInfo = BuildIndexInfo(currentIndex);
        index_build(cudescRel, NULL, currentIndex, NULL, indexInfo, false, true, INDEX_CREATE_NONE_PARTITION);
        index_close(currentIndex, NoLock);
    }

    heap_close(cudescRel, NoLock);
}

/*
 * RelationSetNewRelfilenode
 *
 * Assign a new relfilenode (physical file name) to the relation.
 *
 * This allows a full rewrite of the relation to be done with transactional
 * safety (since the filenode assignment can be rolled back).  Note however
 * that there is no simple way to access the relation's old data for the
 * remainder of the current transaction.  This limits the usefulness to cases
 * such as TRUNCATE or rebuilding an index from scratch.
 *
 * Caller must already hold exclusive lock on the relation.
 *
 * The relation is marked with relfrozenxid = freezeXid (InvalidTransactionId
 * must be passed for indexes and sequences).  This should be a lower bound on
 * the XIDs that will be put into the new relation contents.
 */
void RelationSetNewRelfilenode(Relation relation, TransactionId freezeXid, MultiXactId minmulti, bool isDfsTruncate)
{
    Oid newrelfilenode;
    RelFileNodeBackend newrnode;
    Relation pg_class;
    HeapTuple tuple;
    HeapTuple nctup;
    Form_pg_class classform;
    Datum values[Natts_pg_class];
    bool nulls[Natts_pg_class];
    bool replaces[Natts_pg_class];
    errno_t rc = EOK;
    bool modifyPgClass = !RELATION_IS_GLOBAL_TEMP(relation);
    bool isbucket;
    /* Indexes, sequences must have Invalid frozenxid; other rels must not */
    Assert(((RelationIsIndex(relation) || RELKIND_IS_SEQUENCE(relation->rd_rel->relkind))
                   ? freezeXid == InvalidTransactionId
                   : TransactionIdIsNormal(freezeXid)) ||
           relation->rd_rel->relkind == RELKIND_RELATION);

    /* Allocate a new relfilenode */
    if (!IsSegmentFileNode(relation->rd_node)) {
        newrelfilenode = GetNewRelFileNode(relation->rd_rel->reltablespace, NULL, relation->rd_rel->relpersistence);
    } else {
        /* segment storage */
        isbucket = BUCKET_OID_IS_VALID(relation->rd_bucketoid) && !RelationIsCrossBucketIndex(relation);
        newrelfilenode = seg_alloc_segment(ConvertToRelfilenodeTblspcOid(relation->rd_rel->reltablespace),
            u_sess->proc_cxt.MyDatabaseId, isbucket, InvalidBlockNumber);
    }
    
    // We must consider cudesc relation and delta relation when it is a CStore relation
    // We skip main partitioned table for setting new filenode, as relcudescrelid and reldeltarelid are zero when
    // constructing partitioned table.
    if (RelationIsColStore(relation)) {
        // step 1: CUDesc relation must set new relfilenode
        // step 2: CUDesc index must be set new relfilenode
        //
        if (!RelationIsPartitioned(relation)) {
            DescTableSetNewRelfilenode(relation->rd_rel->relcudescrelid, freezeXid, false);

            // Step 3: Deta relation must be set new relfilenode
            //
            DeltaTableSetNewRelfilenode(relation->rd_rel->reldeltarelid, freezeXid, false);
        }
        /* Both PAXformat and CUFormat will enther this logic
         * Only if it's CUFormat, we need to do step 4
         */
        if (RelationIsCUFormat(relation)) {
            // Step 4: Create first data file for newrelfilenode
            // Note that we need add xlog when create file
            //
            CStore::CreateStorage(relation, newrelfilenode);
        }
    }

    if (modifyPgClass) {
        /*
        * Get a writable copy of the pg_class tuple for the given relation.
        */
        pg_class = heap_open(RelationRelationId, RowExclusiveLock);
        tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(relation)));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not find tuple for relation %u", RelationGetRelid(relation))));
        }
        classform = (Form_pg_class) GETSTRUCT(tuple);
    }
    ereport(LOG,
        (errmsg("Relation %s(%u) set newfilenode %u oldfilenode %u xid %lu",
            RelationGetRelationName(relation),
            RelationGetRelid(relation),
            newrelfilenode,
            relation->rd_node.relNode,
            GetCurrentTransactionIdIfAny())));

    /*
     * Create storage for the main fork of the new relfilenode.
     *
     * NOTE: any conflict in relfilenode value will be caught here, if
     * GetNewRelFileNode messes up for any reason.
     */
    newrnode.node = relation->rd_node;
    newrnode.node.relNode = newrelfilenode;
    newrnode.backend = relation->rd_backend;

    if (RelationIsCrossBucketIndex(relation)) {
        relation->newcbi = true;
    }

    RelationCreateStorage(newrnode.node, relation->rd_rel->relpersistence,
        relation->rd_rel->relowner,
        RelationIsPartitioned(relation) ? InvalidOid : relation->rd_bucketoid,
        relation);
    smgrclosenode(newrnode);

    /*
     * Schedule unlinking of the old storage at transaction commit.
     */
    RelationDropStorage(relation, isDfsTruncate);

    if (!modifyPgClass) {
        Oid relnode = gtt_fetch_current_relfilenode(RelationGetRelid(relation));
        Assert(RELATION_IS_GLOBAL_TEMP(relation));
        Assert(!RelationIsMapped(relation));
        relation->rd_node.relNode = relnode;
        CacheInvalidateRelcache(relation);
    } else {
        /*
         * Now update the pg_class row.  However, if we're dealing with a mapped
         * index, pg_class.relfilenode doesn't change; instead we have to send the
         * update to the relation mapper.
         */
        if (RelationIsMapped(relation))
            RelationMapUpdateMap(RelationGetRelid(relation), newrelfilenode, relation->rd_rel->relisshared, false);
        else
            classform->relfilenode = newrelfilenode;

        /* These changes are safe even for a mapped relation */
        if (!RELKIND_IS_SEQUENCE(relation->rd_rel->relkind)) {
            classform->relpages = 0; /* it's empty until further notice */
            classform->reltuples = 0;
            classform->relallvisible = 0;
        }
        /* set classform's relfrozenxid and relfrozenxid64 */
        classform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
        values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(freezeXid);

#ifndef ENABLE_MULTIPLE_NODES
        replaces[Anum_pg_class_relminmxid - 1] = true;
        values[Anum_pg_class_relminmxid - 1] = TransactionIdGetDatum(minmulti);
#endif

        nctup = heap_modify_tuple(tuple, RelationGetDescr(pg_class), values, nulls, replaces);

        simple_heap_update(pg_class, &nctup->t_self, nctup);
        CatalogUpdateIndexes(pg_class, nctup);

        heap_freetuple_ext(nctup);
        heap_freetuple_ext(tuple);
        heap_close(pg_class, RowExclusiveLock);
    }

    /*
     * Make the pg_class row change visible, as well as the relation map
     * change if any.  This will cause the relcache entry to get updated, too.
     */
    CommandCounterIncrement();

    /*
     * Mark the rel as having been given a new relfilenode in the current
     * (sub) transaction.  This is a hint that can be used to optimize later
     * operations on the rel in the same transaction.
     */
    relation->rd_newRelfilenodeSubid = GetCurrentSubTransactionId();
    /* ... and now we have eoxact cleanup work to do */
    SetRelCacheNeedEOXActWork(true);
}

RelFileNodeBackend CreateNewRelfilenode(Relation relation, TransactionId freezeXid)
{
    Oid newrelfilenode;
    RelFileNodeBackend newrnode;

    /* Allocate a new relfilenode */
    /*
     * Create storage for the main fork of the new relfilenode.
     *
     * NOTE: any conflict in relfilenode value will be caught here, if
     * GetNewRelFileNode messes up for any reason.
     */

    newrelfilenode = GetNewRelFileNode(relation->rd_rel->reltablespace, NULL, relation->rd_rel->relpersistence);

    newrnode.node = relation->rd_node;
    newrnode.node.relNode = newrelfilenode;
    newrnode.backend = relation->rd_backend;
    RelationCreateStorage(
        newrnode.node, relation->rd_rel->relpersistence, relation->rd_rel->relowner, relation->rd_bucketoid);
    smgrclosenode(newrnode);

    return newrnode;
}

RelFileNodeBackend CreateNewRelfilenodePart(Relation parent, Partition part)
{
    Oid newrelfilenode;
    RelFileNodeBackend newrnode;

    /* Allocate a new relfilenode */
    /*
     * Create storage for the main fork of the new relfilenode.
     *
     * NOTE: any conflict in relfilenode value will be caught here, if
     * GetNewRelFileNode messes up for any reason.
     */

    newrelfilenode = GetNewRelFileNode(part->pd_part->reltablespace, NULL, parent->rd_rel->relpersistence);

    newrnode.node = part->pd_node;
    newrnode.node.relNode = newrelfilenode;
    newrnode.backend = parent->rd_backend;
    if (RelationIsCrossBucketIndex(parent)) {
        part->newcbi = true;
    }

    partition_create_new_storage(parent, part, newrnode);

    return newrnode;
}


void UpdatePgclass(Relation relation, TransactionId freezeXid, const RelFileNodeBackend *rnode)
{
    Relation pg_class;
    HeapTuple tuple;
    Form_pg_class classform;
    HeapTuple nctup;
    Datum values[Natts_pg_class];
    bool nulls[Natts_pg_class];
    bool replaces[Natts_pg_class];
    errno_t rc;

    pg_class = heap_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(relation)));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not find tuple for relation %u", RelationGetRelid(relation))));
    classform = (Form_pg_class)GETSTRUCT(tuple);

    ereport(LOG,
        (errmsg("Relation %s(%u) set newfilenode %u oldfilenode %u xid %lu",
            RelationGetRelationName(relation),
            RelationGetRelid(relation),
            rnode->node.relNode,
            relation->rd_node.relNode,
            GetCurrentTransactionIdIfAny())));

    classform->relfilenode = rnode->node.relNode;

    /* These changes are safe even for a mapped relation */
    if (!RELKIND_IS_SEQUENCE(relation->rd_rel->relkind)) {
        classform->relpages = 0; /* it's empty until further notice */
        classform->reltuples = 0;
        classform->relallvisible = 0;
    }

    /* set classform's relfrozenxid and relfrozenxid64 */
    classform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_class_relfrozenxid64 - 1] = true;
    values[Anum_pg_class_relfrozenxid64 - 1] = TransactionIdGetDatum(freezeXid);

    nctup = heap_modify_tuple(tuple, RelationGetDescr(pg_class), values, nulls, replaces);

    simple_heap_update(pg_class, &nctup->t_self, nctup);
    CatalogUpdateIndexes(pg_class, nctup);

    heap_freetuple_ext(nctup);
    heap_freetuple_ext(tuple);

    heap_close(pg_class, RowExclusiveLock);

    return;
}

void UpdatePartition(Relation parent, Partition part, TransactionId freezeXid, const RelFileNodeBackend *newrnode)
{
    Relation pg_partition;
    HeapTuple tuple;
    HeapTuple ntup;
    Form_pg_partition partform;
    Datum values[Natts_pg_partition];
    bool nulls[Natts_pg_partition];
    bool replaces[Natts_pg_partition];
    errno_t rc;

    /*
     * Get a writable copy of the pg_partition tuple for the given relation.
     */
    pg_partition = heap_open(PartitionRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(PartitionGetPartid(part)));

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("could not find tuple for partition %u", PartitionGetPartid(part))));
    }
    partform = (Form_pg_partition)GETSTRUCT(tuple);

    ereport(LOG,
        (errmsg("Partition %s(%u) set newfilenode %u oldfilenode %u xid %lu",
            PartitionGetPartitionName(part),
            PartitionGetPartid(part),
            newrnode->node.relNode,
            part->pd_node.relNode,
            GetCurrentTransactionIdIfAny())));


    Assert(!((part)->pd_part->relfilenode == InvalidOid));
    partform->relfilenode = newrnode->node.relNode;

    Assert(!RELKIND_IS_SEQUENCE(parent->rd_rel->relkind));
    partform->relpages = 0; /* it's empty until further notice */
    partform->reltuples = 0;
    partform->relallvisible = 0;

    /* set relfrozenxid64 */
    partform->relfrozenxid = (ShortTransactionId)InvalidTransactionId;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pg_partition_relfrozenxid64 - 1] = true;
    values[Anum_pg_partition_relfrozenxid64 - 1] = TransactionIdGetDatum(freezeXid);

    ntup = heap_modify_tuple(tuple, RelationGetDescr(pg_partition), values, nulls, replaces);

    simple_heap_update(pg_partition, &ntup->t_self, ntup);
    CatalogUpdateIndexes(pg_partition, ntup);

    heap_freetuple_ext(ntup);
    heap_freetuple_ext(tuple);

    heap_close(pg_partition, RowExclusiveLock);

    return;
}

/*
 *		RelationCacheInitialize
 *
 *		This initializes the relation descriptor cache.  At the time
 *		that this is invoked, we can't do database access yet (mainly
 *		because the transaction subsystem is not up); all we are doing
 *		is making an empty cache hashtable.  This must be done before
 *		starting the initialization transaction, because otherwise
 *		AtEOXact_RelationCache would crash if that transaction aborts
 *		before we can get the relcache set up.
 */

#define INITRELCACHESIZE 400
void RelationCacheInitialize(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.Init();
        return;
    }
    HASHCTL ctl;

    /*
     * create hashtable that indexes the relcache
     */
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(RelIdCacheEnt);
    ctl.hash = oid_hash;
    ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->relcache_cxt.RelationIdCache =
        hash_create("Relcache by OID", INITRELCACHESIZE, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /*
     * relation mapper needs to be initialized too
     */
    RelationMapInitialize();
}

/*
 *		RelationCacheInitializePhase2
 *
 *		This is called to prepare for access to shared catalogs during startup.
 *		We must at least set up nailed reldescs for pg_database, pg_authid,
 *		and pg_auth_members.  Ideally we'd like to have reldescs for their
 *		indexes, too.  We attempt to load this information from the shared
 *		relcache init file.  If that's missing or broken, just make phony
 *		entries for the catalogs themselves.  RelationCacheInitializePhase3
 *		will clean up as needed.
 */

void RelationCacheInitializePhase2(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.InitPhase2();
        return;
    }
    MemoryContext oldcxt;

    /*
     * relation mapper needs initialized too
     */
    RelationMapInitializePhase2();

    /*
     * In bootstrap mode, the shared catalogs aren't there yet anyway, so do
     * nothing.
     */
    if (IsBootstrapProcessingMode())
        return;

    /*
     * switch to cache memory context
     */
    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

    /*
     * Try to load the shared relcache cache file.	If unsuccessful, bootstrap
     * the cache with pre-made descriptors for the critical shared catalogs.
     */
    if (!load_relcache_init_file(true)) {
        formrdesc("pg_database", DatabaseRelation_Rowtype_Id, true, true, Natts_pg_database, Desc_pg_database);
        formrdesc("pg_authid", AuthIdRelation_Rowtype_Id, true, true, Natts_pg_authid, Desc_pg_authid);
        formrdesc(
            "pg_auth_members", AuthMemRelation_Rowtype_Id, true, false, Natts_pg_auth_members, Desc_pg_auth_members);
        formrdesc(
            "pg_user_status", UserStatusRelation_Rowtype_Id, true, true, Natts_pg_user_status, Desc_pg_user_status);
#define NUM_CRITICAL_SHARED_RELS 4 /* fix if you change list above */
    }

    (void)MemoryContextSwitchTo(oldcxt);
}

void RelationCacheInvalidOid(Relation relation)
{
    HeapTuple htup;
    Form_pg_class relp;
    int natts = 0;

    htup = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(relation)));
    if (!HeapTupleIsValid(htup))
        ereport(FATAL, (errmsg("cache lookup failed for relation %u", RelationGetRelid(relation))));
    relp = (Form_pg_class)GETSTRUCT(htup);

    /*
     * Copy tuple to relation->rd_rel. (See notes in
     * AllocateRelationDesc())
     * But pay attention to relnatts of rd_rel because we hardcoded all system catalogs,
     * relnatts in pg_class might be old and will not be updated.
     * Use need to use hardcoded info in schemapg.h to fix it.
     */
    natts = relation->rd_rel->relnatts;
    errno_t rc = memcpy_s((char*)relation->rd_rel, CLASS_TUPLE_SIZE, (char*)relp, CLASS_TUPLE_SIZE);
    securec_check(rc, "\0", "\0");
    relation->rd_rel->relnatts = natts;

    /* Update rd_options while we have the tuple */
    if (relation->rd_options)
        pfree_ext(relation->rd_options);
    RelationParseRelOptions(relation, htup);

    /*
     * Check the values in rd_att were set up correctly.  (We cannot
     * just copy them over now: formrdesc must have set up the rd_att
     * data correctly to start with, because it may already have been
     * copied into one or more catcache entries.)
     */
    Assert(relation->rd_att->tdtypeid == relp->reltype);
    Assert(relation->rd_att->tdtypmod == -1);
    Assert(relation->rd_att->tdhasoid == relp->relhasoids);

    ReleaseSysCache(htup);
}

/*
 *		RelationCacheInitializePhase3
 *
 *		This is called as soon as the catcache and transaction system
 *		are functional and we have determined u_sess->proc_cxt.MyDatabaseId.  At this point
 *		we can actually read data from the database's system catalogs.
 *		We first try to read pre-computed relcache entries from the local
 *		relcache init file.  If that's missing or broken, make phony entries
 *		for the minimum set of nailed-in-cache relations.  Then (unless
 *		bootstrapping) make sure we have entries for the critical system
 *		indexes.  Once we've done all this, we have enough infrastructure to
 *		open any system catalog or use any catcache.  The last step is to
 *		rewrite the cache files if needed.
 */

void RelationCacheInitializePhase3(void)
{
    if (EnableLocalSysCache()) {
        t_thrd.lsc_cxt.lsc->tabdefcache.InitPhase3();
        return;
    }
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;
    MemoryContext oldcxt;
    bool needNewCacheFile = !u_sess->relcache_cxt.criticalSharedRelcachesBuilt;

    /*
     * relation mapper needs initialized too
     */
    RelationMapInitializePhase3();

    /*
     * switch to cache memory context
     */
    oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);

    /*
     * Try to load the local relcache cache file.  If unsuccessful, bootstrap
     * the cache with pre-made descriptors for the critical "nailed-in" system
     * catalogs.
     *
     * Vacuum-full pg_class will move entries of indexes on pg_class to the end
     * of pg_class. When bootstraping critical catcaches and relcached from scratch,
     * we have to do sequential scan for these entries. For databases with millions
     * of pg_class entries, this could take quite a long time. To make matters worse,
     * when multiple backends parallelly do this, they may block one another severly
     * due to lock on the same share buffer partition. To avoid such case, we only allow
     * one backend to bootstrap critical catcaches at one time. When it is done, the other
     * parallel backends would first try to load from init file once again.
     * Since share catalogs usually have much less entries, we only do so for local catalogs
     * at present.
     */
retry:
    if (IsBootstrapProcessingMode() || !load_relcache_init_file(false)) {
        if (!IsBootstrapProcessingMode()) {
            uint32 state = 0;
            if (pg_atomic_compare_exchange_u32(
                    &t_thrd.xact_cxt.ShmemVariableCache->CriticalCacheBuildLock, &state, 1)) {
                /* Get the lock */
                needNewLocalCacheFile = true;
            } else {
                /* Someone hold the lock, sleep and retry */
                pg_usleep(10000); /* 10ms */
                goto retry;
            }
        }

        if (IsBootstrapProcessingMode() || !load_relcache_init_file(false)) {
            needNewCacheFile = true;

            formrdesc("pg_class", RelationRelation_Rowtype_Id, false, true, Natts_pg_class, Desc_pg_class);
            formrdesc(
                "pg_attribute", AttributeRelation_Rowtype_Id, false, false, Natts_pg_attribute, Desc_pg_attribute);
            formrdesc("pg_proc", ProcedureRelation_Rowtype_Id, false, true, Natts_pg_proc, Desc_pg_proc);
            formrdesc("pg_type", TypeRelation_Rowtype_Id, false, true, Natts_pg_type, Desc_pg_type);
        } else {
            Assert(needNewLocalCacheFile);
            needNewLocalCacheFile = false;
            pg_atomic_exchange_u32(&t_thrd.xact_cxt.ShmemVariableCache->CriticalCacheBuildLock, 0);
        }

#define NUM_CRITICAL_LOCAL_RELS 4 /* fix if you change list above */
    }

    (void)MemoryContextSwitchTo(oldcxt);

    /* In bootstrap mode, the faked-up formrdesc info is all we'll have */
    if (IsBootstrapProcessingMode())
        return;

    /*
     * If we didn't get the critical system indexes loaded into relcache, do
     * so now.	These are critical because the catcache and/or opclass cache
     * depend on them for fetches done during relcache load.  Thus, we have an
     * infinite-recursion problem.	We can break the recursion by doing
     * heapscans instead of indexscans at certain key spots. To avoid hobbling
     * performance, we only want to do that until we have the critical indexes
     * loaded into relcache.  Thus, the flag u_sess->relcache_cxt.criticalRelcachesBuilt is used to
     * decide whether to do heapscan or indexscan at the key spots, and we set
     * it true after we've loaded the critical indexes.
     *
     * The critical indexes are marked as "nailed in cache", partly to make it
     * easy for load_relcache_init_file to count them, but mainly because we
     * cannot flush and rebuild them once we've set u_sess->relcache_cxt.criticalRelcachesBuilt to
     * true.  (NOTE: perhaps it would be possible to reload them by
     * temporarily setting u_sess->relcache_cxt.criticalRelcachesBuilt to false again.  For now,
     * though, we just nail 'em in.)
     *
     * RewriteRelRulenameIndexId and TriggerRelidNameIndexId are not critical
     * in the same way as the others, because the critical catalogs don't
     * (currently) have any rules or triggers, and so these indexes can be
     * rebuilt without inducing recursion.	However they are used during
     * relcache load when a rel does have rules or triggers, so we choose to
     * nail them for performance reasons.
     */
    if (!u_sess->relcache_cxt.criticalRelcachesBuilt) {
        load_critical_index(ClassOidIndexId, RelationRelationId);
        load_critical_index(AttributeRelidNumIndexId, AttributeRelationId);
        load_critical_index(IndexRelidIndexId, IndexRelationId);
        load_critical_index(OpclassOidIndexId, OperatorClassRelationId);
        load_critical_index(AccessMethodProcedureIndexId, AccessMethodProcedureRelationId);
        load_critical_index(RewriteRelRulenameIndexId, RewriteRelationId);
        load_critical_index(TriggerRelidNameIndexId, TriggerRelationId);

#define NUM_CRITICAL_LOCAL_INDEXES 7 /* fix if you change list above */
        u_sess->relcache_cxt.criticalRelcachesBuilt = true;
    }

    /*
     * Process critical shared indexes too.
     *
     * DatabaseNameIndexId isn't critical for relcache loading, but rather for
     * initial lookup of u_sess->proc_cxt.MyDatabaseId, without which we'll never find any
     * non-shared catalogs at all.	Autovacuum calls InitPostgres with a
     * database OID, so it instead depends on DatabaseOidIndexId.  We also
     * need to nail up some indexes on pg_authid and pg_auth_members for use
     * during client authentication.
     */
    if (!u_sess->relcache_cxt.criticalSharedRelcachesBuilt) {
        load_critical_index(DatabaseNameIndexId, DatabaseRelationId);
        load_critical_index(DatabaseOidIndexId, DatabaseRelationId);
        load_critical_index(AuthIdRolnameIndexId, AuthIdRelationId);
        load_critical_index(AuthIdOidIndexId, AuthIdRelationId);
        load_critical_index(AuthMemMemRoleIndexId, AuthMemRelationId);
        load_critical_index(UserStatusRoleidIndexId, UserStatusRelationId);

#define NUM_CRITICAL_SHARED_INDEXES 6 /* fix if you change list above */

        u_sess->relcache_cxt.criticalSharedRelcachesBuilt = true;
    }

    /*
     * Now, scan all the relcache entries and update anything that might be
     * wrong in the results from formrdesc or the relcache cache file. If we
     * faked up relcache entries using formrdesc, then read the real pg_class
     * rows and replace the fake entries with them. Also, if any of the
     * relcache entries have rules or triggers, load that info the hard way
     * since it isn't recorded in the cache file.
     *
     * Whenever we access the catalogs to read data, there is a possibility of
     * a shared-inval cache flush causing relcache entries to be removed.
     * Since hash_seq_search only guarantees to still work after the *current*
     * entry is removed, it's unsafe to continue the hashtable scan afterward.
     * We handle this by restarting the scan from scratch after each access.
     * This is theoretically O(N^2), but the number of entries that actually
     * need to be fixed is small enough that it doesn't matter.
     */
    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Relation relation = idhentry->reldesc;
        bool restart = false;

        /*
         * Make sure *this* entry doesn't get flushed while we work with it.
         */
        RelationIncrementReferenceCount(relation);

        /*
         * If it's a faked-up entry, read the real pg_class tuple.
         */
        if (relation->rd_rel->relowner == InvalidOid) {
            RelationCacheInvalidOid(relation);

            /* relowner had better be OK now, else we'll loop forever */
            if (relation->rd_rel->relowner == InvalidOid)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid relowner in pg_class entry for \"%s\"", RelationGetRelationName(relation))));

            restart = true;
        }

        /*
         * Fix data that isn't saved in relcache cache file.
         *
         * relhasrules or relhastriggers could possibly be wrong or out of
         * date.  If we don't actually find any rules or triggers, clear the
         * local copy of the flag so that we don't get into an infinite loop
         * here.  We don't make any attempt to fix the pg_class entry, though.
         */
        if (relation->rd_rel->relhasrules && relation->rd_rules == NULL) {
            RelationBuildRuleLock(relation);
            if (relation->rd_rules == NULL)
                relation->rd_rel->relhasrules = false;
            restart = true;
        }
        if (relation->rd_rel->relhastriggers && relation->trigdesc == NULL) {
            RelationBuildTriggers(relation);
            if (relation->trigdesc == NULL)
                relation->rd_rel->relhastriggers = false;
            restart = true;
        }

        /* get row level security policies for this relation */
        if (RelationEnableRowSecurity(relation) && relation->rd_rlsdesc == NULL) {
            RelationBuildRlsPolicies(relation);
            Assert(relation->rd_rlsdesc != NULL);
            restart = true;
        }

        /* Release hold on the relation */
        RelationDecrementReferenceCount(relation);

        /* Now, restart the hashtable scan if needed */
        if (restart) {
            hash_seq_term(&status);
            hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);
        }
    }

    /*
     * Lastly, write out new relcache cache files if needed.  We don't bother
     * to distinguish cases where only one of the two needs an update.
     */
    if (needNewCacheFile) {
        /*
         * Force all the catcaches to finish initializing and thereby open the
         * catalogs and indexes they use.  This will preload the relcache with
         * entries for all the most important system catalogs and indexes, so
         * that the init files will be most useful for future backends.
         */
        InitCatalogCachePhase2();

        /* reset t_thrd.relcache_cxt.initFileRelationIds list; we'll fill it during write */
        u_sess->relcache_cxt.initFileRelationIds = NIL;

        /* now write the files */
        write_relcache_init_file(true);
        write_relcache_init_file(false);

        if (needNewLocalCacheFile) {
            needNewLocalCacheFile = false;
            pg_atomic_exchange_u32(&t_thrd.xact_cxt.ShmemVariableCache->CriticalCacheBuildLock, 0);
        }
    }
}

/*
 * Load one critical system index into the relcache
 *
 * indexoid is the OID of the target index, heapoid is the OID of the catalog
 * it belongs to.
 */
extern Relation load_critical_index(Oid indexoid, Oid heapoid)
{
    Relation ird;
    int curRetryCnt = 0;
    int const maxRetryCnt = 10;

retry_if_standby_mode:
    /*
     * We must lock the underlying catalog before locking the index to avoid
     * deadlock, since RelationBuildDesc might well need to read the catalog,
     * and if anyone else is exclusive-locking this catalog and index they'll
     * be doing it in that order.
     */
    LockRelationOid(heapoid, AccessShareLock);
    LockRelationOid(indexoid, AccessShareLock);
    ird = RelationBuildDesc(indexoid, true);
    if (ird == NULL) {
        /*
         * in standby mode real minRecoveryPoint may not be reached.
         * see branch XLByteLT(EndRecPtr, minRecoveryPoint) in function XLogPageRead()
         * so we have to retry in standby mode.
         */
        if (IsServerModeStandby() && curRetryCnt < maxRetryCnt) {
            /* release index && heap lock before retry again */
            UnlockRelationOid(indexoid, AccessShareLock);
            UnlockRelationOid(heapoid, AccessShareLock);
            if (0 == curRetryCnt++) {
                ereport(LOG, (errmsg("the first fail to open critical system index %u of heap %u", indexoid, heapoid)));
            }
            pg_usleep(1000000);
            goto retry_if_standby_mode;
        }
        ereport(PANIC, (errmsg("could not open critical system index %u", indexoid)));
    } else if (0 != curRetryCnt) {
        ereport(LOG,
            (errmsg("the last fail to open critical system index %u of heap %u, count %d",
                indexoid,
                heapoid,
                curRetryCnt)));
    }
    ird->rd_isnailed = true;
    ird->rd_refcnt = 1;
    UnlockRelationOid(indexoid, AccessShareLock);
    UnlockRelationOid(heapoid, AccessShareLock);
    return ird;
}
/*
 * GetPgClassDescriptor -- get a predefined tuple descriptor for pg_class
 * GetPgIndexDescriptor -- get a predefined tuple descriptor for pg_index
 *
 * We need this kluge because we have to be able to access non-fixed-width
 * fields of pg_class and pg_index before we have the standard catalog caches
 * available.  We use predefined data that's set up in just the same way as
 * the bootstrapped reldescs used by formrdesc().  The resulting tupdesc is
 * not 100% kosher: it does not have the correct rowtype OID in tdtypeid, nor
 * does it have a TupleConstr field.  But it's good enough for the purpose of
 * extracting fields.
 */
TupleDesc BuildHardcodedDescriptor(int natts, const FormData_pg_attribute* attrs, bool hasoids)
{
    TupleDesc result;
    int i;
    result = CreateTemplateTupleDesc(natts, hasoids, TAM_HEAP);
    result->tdtypeid = RECORDOID; /* not right, but we don't care */
    result->tdtypmod = -1;

    for (i = 0; i < natts; i++) {
        errno_t rc = memcpy_s(result->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, &attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "", "");
        /* make sure attcacheoff is valid */
        result->attrs[i]->attcacheoff = -1;
    }

    /* initialize first attribute's attcacheoff, cf RelationBuildTupleDesc */
    result->attrs[0]->attcacheoff = 0;

    /* Note: we don't bother to set up a TupleConstr entry */

    return result;
}

extern TupleDesc GetPgClassDescriptor(void)
{
    /* Already done? */
    if (u_sess->relcache_cxt.pgclassdesc == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        u_sess->relcache_cxt.pgclassdesc = BuildHardcodedDescriptor(Natts_pg_class, Desc_pg_class, true);
        (void)MemoryContextSwitchTo(oldcxt);
    }

    return u_sess->relcache_cxt.pgclassdesc;
}

/*
 * Replace with GetPgClassDescriptor
 *
 * Ban to release return value since it is a static value, and it is used
 * frequently in relation operation
 */
TupleDesc GetDefaultPgClassDesc(void)
{
    return GetLSCPgClassDescriptor();
}

TupleDesc GetPgIndexDescriptor(void)
{
    /* Already done? */
    if (u_sess->relcache_cxt.pgindexdesc == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        u_sess->relcache_cxt.pgindexdesc = BuildHardcodedDescriptor(Natts_pg_index, Desc_pg_index, false);
        (void)MemoryContextSwitchTo(oldcxt);
    }

    return u_sess->relcache_cxt.pgindexdesc;
}

/*
 * Replace with get_pg_index_descriptor
 *
 * Ban to release return value since it is a static value, and it is used
 * frequently in relation operation
 */
TupleDesc GetDefaultPgIndexDesc(void)
{
    return GetLSCPgIndexDescriptor();
}

/*
 * Load generated column attribute value definitions for the relation.
 */
static void GeneratedColFetch(TupleConstr *constr, HeapTuple htup, Relation adrel, int attrdefIndex)
{
    char *genCols = constr->generatedCols;
    AttrDefault *attrdef = constr->defval;
    char generatedCol = '\0';
    if (HeapTupleHeaderGetNatts(htup->t_data, adrel->rd_att) >= Anum_pg_attrdef_adgencol) {
        bool isnull = false;
        Datum val = fastgetattr(htup, Anum_pg_attrdef_adgencol, adrel->rd_att, &isnull);
        if (!isnull) {
            generatedCol = DatumGetChar(val);
        }
    }
    attrdef[attrdefIndex].generatedCol = generatedCol;
    genCols[attrdef[attrdefIndex].adnum - 1] = generatedCol;
    if (generatedCol == ATTRIBUTE_GENERATED_STORED) {
        constr->has_generated_stored = true;
    }
}

/*
 * Load any default attribute value definitions for the relation.
 */
static void AttrDefaultFetch(Relation relation)
{
    AttrDefault *attrdef = relation->rd_att->constr->defval;
    int ndef = relation->rd_att->constr->num_defval;
    ScanKeyData skey;
    HeapTuple htup;
    Datum val;
    bool isnull = false;
    int i;
    int found = 0;

    ScanKeyInit(&skey, Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)));
    Relation adrel = heap_open(AttrDefaultRelationId, AccessShareLock);
    SysScanDesc adscan = systable_beginscan(adrel, AttrDefaultIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(adscan))) {
        Form_pg_attrdef adform = (Form_pg_attrdef)GETSTRUCT(htup);

        for (i = 0; i < ndef; i++) {
            if (adform->adnum != attrdef[i].adnum)
                continue;

            if (attrdef[i].adbin != NULL)
                ereport(WARNING, (errmsg("multiple attrdef records found for attr %s of rel %s",
                    NameStr(relation->rd_att->attrs[adform->adnum - 1]->attname), RelationGetRelationName(relation))));
            else
                found++;

            if (t_thrd.proc->workingVersionNum >= GENERATED_COL_VERSION_NUM) {
                GeneratedColFetch(relation->rd_att->constr, htup, adrel, i);
            }

            val = fastgetattr(htup, Anum_pg_attrdef_adbin, adrel->rd_att, &isnull);
            if (isnull)
                ereport(WARNING, (errmsg("null adbin for attr %s of rel %s",
                    NameStr(relation->rd_att->attrs[adform->adnum - 1]->attname), RelationGetRelationName(relation))));
            else
                attrdef[i].adbin = MemoryContextStrdup(LocalMyDBCacheMemCxt(), TextDatumGetCString(val));
            break;
        }

        if (i >= ndef) {
            ereport(WARNING, (errmsg("unexpected attrdef record found for attr %d of rel %s", adform->adnum,
                RelationGetRelationName(relation))));
        }
    }

    systable_endscan(adscan);
    heap_close(adrel, AccessShareLock);

    if (found != ndef)
        ereport(WARNING,
            (errmsg("%d attrdef record(s) missing for rel %s", ndef - found, RelationGetRelationName(relation))));
}

/*
 * Load any check constraints for the relation.
 */
static void CheckConstraintFetch(Relation relation)
{
    ConstrCheck* check = relation->rd_att->constr->check;
    int ncheck = relation->rd_att->constr->num_check;
    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData skey[1];
    HeapTuple htup;
    Datum val;
    bool isnull = false;
    int found = 0;

    ScanKeyInit(&skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)));

    conrel = heap_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        Form_pg_constraint conform = (Form_pg_constraint)GETSTRUCT(htup);

        /* We want check constraints only */
        if (conform->contype != CONSTRAINT_CHECK)
            continue;

        if (found >= ncheck)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("unexpected constraint record found for rel %s", RelationGetRelationName(relation))));

        check[found].ccvalid = conform->convalidated;
        check[found].ccnoinherit = conform->connoinherit;
        check[found].ccname = MemoryContextStrdup(LocalMyDBCacheMemCxt(), NameStr(conform->conname));

        /* Grab and test conbin is actually set */
        val = fastgetattr(htup, Anum_pg_constraint_conbin, conrel->rd_att, &isnull);
        if (isnull)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("null conbin for rel %s", RelationGetRelationName(relation))));

        check[found].ccbin = MemoryContextStrdup(LocalMyDBCacheMemCxt(), TextDatumGetCString(val));
        found++;
    }

    systable_endscan(conscan);
    heap_close(conrel, AccessShareLock);

    if (found != ncheck)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg(
                    "%d constraint record(s) missing for rel %s", ncheck - found, RelationGetRelationName(relation))));
}

void SaveCopyList(Relation relation, List* result, int oidIndex)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    if (relation->rd_indexlist) {
        list_free_ext(relation->rd_indexlist);
    }
    relation->rd_indexlist = list_copy(result);
    relation->rd_oidindex = oidIndex;
    relation->rd_indexvalid = 1;
    (void)MemoryContextSwitchTo(oldcxt);
}

/*
 * RelationGetSpecificKindIndexList -- get a list of OIDs of global indexes on this relation or not
 */
List* RelationGetSpecificKindIndexList(Relation relation, bool isGlobal)
{
    ListCell* indList = NULL;
    List* result = NULL;
    List* indexOidList = RelationGetIndexList(relation);

    /* Ask the relcache to produce a list of the indexes of the rel */
    foreach (indList, indexOidList) {
        Oid indexId = lfirst_oid(indList);
        Relation currentIndex;

        /* Open the index relation; use exclusive lock, just to be sure */
        currentIndex = index_open(indexId, AccessShareLock);
        if (isGlobal) {
            if (RelationIsGlobalIndex(currentIndex)) {
                result = insert_ordered_oid(result, indexId);
            }
        } else {
            if (!RelationIsGlobalIndex(currentIndex)) {
                result = insert_ordered_oid(result, indexId);
            }
        }
        index_close(currentIndex, AccessShareLock);
    }

    list_free_ext(indexOidList);
    return result;
}

List* RelationGetLocalCbiList(Relation relation)
{
    ListCell* indList = NULL;
    List* result = NULL;
    List* indexOidList = RelationGetIndexList(relation);
    /* Ask the relcache to produce a list of the indexes of the rel */
    foreach (indList, indexOidList) {
        Oid indexId = lfirst_oid(indList);
        Relation currentIndex;
        /* Open the index relation; use exclusive lock, just to be sure */
        currentIndex = index_open(indexId, AccessShareLock);
        if (RelationIsCrossBucketIndex(currentIndex) && !RelationIsGlobalIndex(currentIndex)) {
            result = insert_ordered_oid(result, indexId);
        }
        index_close(currentIndex, AccessShareLock);
    }
    list_free_ext(indexOidList);
    return result;
}

/*
 * RelationGetIndexList -- get a list of OIDs of indexes on this relation
 *
 * The index list is created only if someone requests it.  We scan pg_index
 * to find relevant indexes, and add the list to the relcache entry so that
 * we won't have to compute it again.  Note that shared cache inval of a
 * relcache entry will delete the old list and set rd_indexvalid to 0,
 * so that we must recompute the index list on next request.  This handles
 * creation or deletion of an index.
 *
 * Indexes that are marked not IndexIsLive are omitted from the returned list.
 * Such indexes are expected to be dropped momentarily, and should not be
 * touched at all by any caller of this function.
 *
 * The returned list is guaranteed to be sorted in order by OID.  This is
 * needed by the executor, since for index types that we obtain exclusive
 * locks on when updating the index, all backends must lock the indexes in
 * the same order or we will get deadlocks (see ExecOpenIndices()).  Any
 * consistent ordering would do, but ordering by OID is easy.
 *
 * Since shared cache inval causes the relcache's copy of the list to go away,
 * we return a copy of the list palloc'd in the caller's context.  The caller
 * may list_free_ext() the returned list after scanning it. This is necessary
 * since the caller will typically be doing syscache lookups on the relevant
 * indexes, and syscache lookup could cause SI messages to be processed!
 *
 * We also update rd_oidindex, which this module treats as effectively part
 * of the index list.  rd_oidindex is valid when rd_indexvalid isn't zero;
 * it is the pg_class OID of a unique index on OID when the relation has one,
 * and InvalidOid if there is no such index.
 */
List* RelationGetIndexList(Relation relation, bool inc_unused)
{
    Relation indrel;
    SysScanDesc indscan;
    ScanKeyData skey;
    HeapTuple htup;
    List* result = NIL;
    char relreplident;
    Oid oidIndex = InvalidOid;
    Oid pkeyIndex = InvalidOid;
    Oid candidateIndex = InvalidOid;
    Datum replident = (Datum)0;

    Assert(!RelationIsBucket(relation) && !RelationIsPartition(relation));

    /* Quick exit if we already computed the list. */
    if (relation->rd_indexvalid != 0 && !inc_unused)
        return list_copy(relation->rd_indexlist);

    bool isNull = false;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    Oid relid = RelationIsPartition(relation) ? relation->parentId : relation->rd_id;
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("pg_class entry for relid %u vanished during RelationGetIndexList", relid)));
    replident = heap_getattr(tuple, Anum_pg_class_relreplident, RelationGetDescr(rel), &isNull);
    heap_close(rel, AccessShareLock);
    heap_freetuple_ext(tuple);

    if (isNull)
        relreplident = REPLICA_IDENTITY_NOTHING;
    else
        relreplident = CharGetDatum(replident);

    /*
     * We build the list we intend to return (in the caller's context) while
     * doing the scan.	After successfully completing the scan, we copy that
     * list into the relcache entry.  This avoids cache-context memory leakage
     * if we get some sort of error partway through.
     */
    result = NIL;
    oidIndex = InvalidOid;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    ScanKeyInit(
        &skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    indrel = heap_open(IndexRelationId, AccessShareLock);
    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);
        Datum indclassDatum;
        oidvector* indclass = NULL;
        bool isnull = false;

        /*
         * Ignore any indexes that are currently being dropped.  This will
         * prevent them from being searched, inserted into, or considered in
         * HOT-safety decisions.  It's unsafe to touch such an index at all
         * since its catalog entries could disappear at any instant.
         */
        if (!IndexIsLive(index) && !inc_unused)
            continue;

        /* Add index's OID to result list in the proper order */
        result = insert_ordered_oid(result, index->indexrelid);

        /*
         * indclass cannot be referenced directly through the C struct,
         * because it comes after the variable-width indkey field.	Must
         * extract the datum the hard way...
         */
        indclassDatum = heap_getattr(htup, Anum_pg_index_indclass, GetLSCPgIndexDescriptor(), &isnull);
        Assert(!isnull);
        indclass = (oidvector*)DatumGetPointer(indclassDatum);
        /*
         * Invalid, non-unique, non-immediate or predicate indexes aren't
         * interesting for neither oid indexes nor replication identity
         * indexes, so don't check them.
         */
        if (!IndexIsValid(index) || !index->indisunique || !index->indimmediate ||
            !heap_attisnull(htup, Anum_pg_index_indpred, NULL))
            continue;

        /* Check to see if is a usable btree index on OID */
        if (index->indnatts == 1 && index->indkey.values[0] == ObjectIdAttributeNumber &&
            indclass->values[0] == OID_BTREE_OPS_OID)
            oidIndex = index->indexrelid;
        /* always prefer primary keys */
        if (index->indisprimary)
            pkeyIndex = index->indexrelid;

        bool isNull = true;
        bool isreplident = false;
        Datum indisreplident;
        indisreplident = heap_getattr(htup, Anum_pg_index_indisreplident, RelationGetDescr(indrel), &isNull);
        if (!isNull) {
            isreplident = BoolGetDatum(indisreplident);
        }
        /* explicitly chosen index */
        if (isreplident)
            candidateIndex = index->indexrelid;
    }

    systable_endscan(indscan);

    /* primary key */
    if (relreplident == REPLICA_IDENTITY_DEFAULT && OidIsValid(pkeyIndex))
        relation->rd_replidindex = pkeyIndex;
    /* explicitly chosen index */
    else if (relreplident == REPLICA_IDENTITY_INDEX && OidIsValid(candidateIndex))
        relation->rd_replidindex = candidateIndex;
    /* nothing */
    else
        relation->rd_replidindex = InvalidOid;

    heap_close(indrel, AccessShareLock);

    /* Now save a copy of the completed list in the relcache entry. */
    if (!inc_unused) {
        SaveCopyList(relation, result, oidIndex);
        relation->rd_pkindex = pkeyIndex;
    }
    
    return result;
}

/*
 * RelationGetIndexInfoList -- get a list of index info
 *
 */
List* RelationGetIndexInfoList(Relation relation)
{
    List* index_info_list = NIL;
    List* index_oid_list = NIL;
    ListCell* l = NULL;

    /* Fast path if definitely no indexes */
    if (!RelationGetForm(relation)->relhasindex)
        return NULL;

    /*
     * Get cached list of index OIDs
     */
    index_oid_list = RelationGetIndexList(relation);

    /* Fall out if no indexes (but relhasindex was set) */
    if (index_oid_list == NIL)
        return NIL;

    /*
     * For each index, add index info to index_info_list.
     *
     * Note: we consider all indexes returned by RelationGetIndexList, even if
     * they are not indisready or indisvalid.  This is important because an
     * index for which CREATE INDEX CONCURRENTLY has just started must be
     * included in HOT-safety decisions (see README.HOT).  If a DROP INDEX
     * CONCURRENTLY is far enough along that we should ignore the index, it
     * won't be returned at all by RelationGetIndexList.
     */
    foreach (l, index_oid_list) {
        Oid index_oid = lfirst_oid(l);
        Relation index_desc;
        IndexInfo* index_info = NULL;

        index_desc = index_open(index_oid, AccessShareLock);

        /* Extract index key information from the index's pg_index row */
        index_info = BuildIndexInfo(index_desc);

        index_info_list = lappend(index_info_list, index_info);

        index_close(index_desc, AccessShareLock);
    }

    list_free_ext(index_oid_list);
    return index_info_list;
}

/*
 * Get index num from relation, this function is almost the same
 * with RelationGetIndexList, except return the length of index
 * list.
 */
int RelationGetIndexNum(Relation relation)
{
    Relation indrel;
    SysScanDesc indscan;
    ScanKeyData skey;
    HeapTuple htup;
    List* result = NULL;
    Oid oidIndex;

    Assert(!RelationIsBucket(relation) && !RelationIsPartition(relation));

    /* Quick exit if we already computed the list. */
    if (relation->rd_indexvalid != 0)
        return list_length(relation->rd_indexlist);

    /*
     * We build the list we intend to return (in the caller's context) while
     * doing the scan.	After successfully completing the scan, we copy that
     * list into the relcache entry.  This avoids cache-context memory leakage
     * if we get some sort of error partway through.
     */
    result = NIL;
    oidIndex = InvalidOid;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    ScanKeyInit(
        &skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(relation)));

    indrel = heap_open(IndexRelationId, AccessShareLock);
    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);
        Datum indclassDatum;
        oidvector* indclass = NULL;
        bool isnull = false;

        /*
         * Ignore any indexes that are currently being dropped.  This will
         * prevent them from being searched, inserted into, or considered in
         * HOT-safety decisions.  It's unsafe to touch such an index at all
         * since its catalog entries could disappear at any instant.
         */
        if (!IndexIsLive(index))
            continue;

        /* Add index's OID to result list in the proper order */
        result = insert_ordered_oid(result, index->indexrelid);

        /*
         * indclass cannot be referenced directly through the C struct,
         * because it comes after the variable-width indkey field.	Must
         * extract the datum the hard way...
         */
        indclassDatum = heap_getattr(htup, Anum_pg_index_indclass, GetLSCPgIndexDescriptor(), &isnull);
        Assert(!isnull);
        indclass = (oidvector*)DatumGetPointer(indclassDatum);

        /* Check to see if it is a unique, non-partial btree index on OID */
        if (IndexIsValid(index) && index->indnatts == 1 && index->indisunique && index->indimmediate &&
            index->indkey.values[0] == ObjectIdAttributeNumber && indclass->values[0] == OID_BTREE_OPS_OID &&
            heap_attisnull(htup, Anum_pg_index_indpred, NULL))
            oidIndex = index->indexrelid;
    }

    systable_endscan(indscan);
    heap_close(indrel, AccessShareLock);

    /* Now save a copy of the completed list in the relcache entry. */
    SaveCopyList(relation, result, oidIndex);
    list_free_ext(result);

    return list_length(relation->rd_indexlist);
}

/*
 * insert_ordered_oid
 *		Insert a new Oid into a sorted list of Oids, preserving ordering
 *
 * Building the ordered list this way is O(N^2), but with a pretty small
 * constant, so for the number of entries we expect it will probably be
 * faster than trying to apply qsort().  Most tables don't have very many
 * indexes...
 */
static List* insert_ordered_oid(List* list, Oid datum)
{
    ListCell* prev = NULL;

    /* Does the datum belong at the front? */
    if (list == NIL || datum < linitial_oid(list))
        return lcons_oid(datum, list);
    /* No, so find the entry it belongs after */
    prev = list_head(list);
    for (;;) {
        ListCell* curr = lnext(prev);

        if (curr == NULL || datum < lfirst_oid(curr))
            break; /* it belongs after 'prev', before 'curr' */

        prev = curr;
    }
    /* Insert datum into list after 'prev' */
    lappend_cell_oid(list, prev, datum);
    return list;
}

/*
 * RelationSetIndexList -- externally force the index list contents
 *
 * This is used to temporarily override what we think the set of valid
 * indexes is (including the presence or absence of an OID index).
 * The forcing will be valid only until transaction commit or abort.
 *
 * This should only be applied to nailed relations, because in a non-nailed
 * relation the hacked index list could be lost at any time due to SI
 * messages.  In practice it is only used on pg_class (see REINDEX).
 *
 * It is up to the caller to make sure the given list is correctly ordered.
 *
 * We deliberately do not change rd_indexattr here: even when operating
 * with a temporary partial index list, HOT-update decisions must be made
 * correctly with respect to the full index set.  It is up to the caller
 * to ensure that a correct rd_indexattr set has been cached before first
 * calling RelationSetIndexList; else a subsequent inquiry might cause a
 * wrong rd_indexattr set to get computed and cached.
 */
void RelationSetIndexList(Relation relation, List* indexIds, Oid oidIndex)
{
    MemoryContext oldcxt;

    Assert(relation->rd_isnailed);
    /* Copy the list into the cache context (could fail for lack of mem) */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    indexIds = list_copy(indexIds);
    (void)MemoryContextSwitchTo(oldcxt);
    /* Okay to replace old list */
    list_free_ext(relation->rd_indexlist);
    relation->rd_indexlist = indexIds;
    relation->rd_oidindex = oidIndex;
    /*
    * For the moment, assume the target rel hasn't got a pk or replica
    * index. We'll load them on demand in the API that wraps access to them.
    */
    relation->rd_pkindex = InvalidOid;
    relation->rd_indexvalid = 2; /* mark list as forced */
    /* must flag that we have a forced index list */
    SetRelCacheNeedEOXActWork(true);
}

/*
 * RelationGetOidIndex -- get the pg_class OID of the relation's OID index
 *
 * Returns InvalidOid if there is no such index.
 */
Oid RelationGetOidIndex(Relation relation)
{
    List* ilist = NULL;

    /*
     * If relation doesn't have OIDs at all, caller is probably confused. (We
     * could just silently return InvalidOid, but it seems better to throw an
     * assertion.)
     */
    Assert(relation->rd_rel->relhasoids);
    Assert(!RelationIsBucket(relation) && !RelationIsPartition(relation));
    if (relation->rd_indexvalid == 0) {
        /* RelationGetIndexList does the heavy lifting. */
        ilist = RelationGetIndexList(relation);
        list_free_ext(ilist);
        Assert(relation->rd_indexvalid != 0);
    }

    return relation->rd_oidindex;
}

/*
 * RelationGetPrimaryKeyIndex -- get OID of the relation's primary key index
 *
 * Returns InvalidOid if there is no such index.
 */
Oid RelationGetPrimaryKeyIndex(Relation relation)
{
    List* ilist;

    if (relation->rd_indexvalid == 0) {
        /* RelationGetIndexList does the heavy lifting. */
        ilist = RelationGetIndexList(relation);
        list_free(ilist);
        Assert(relation->rd_indexvalid != 0);
    }

    return relation->rd_pkindex;
}

/*
 * RelationGetReplicaIndex -- get OID of the relation's replica identity index
 * If the table is partition table, return the replica identity index of parent table.
 *
 * Returns InvalidOid if there is no such index.
 */
Oid RelationGetReplicaIndex(Relation relation)
{
    List* ilist = NIL;
    Relation parentRelation;
    Oid replidindex;
    
    if (RelationIsBucket(relation)) {
        parentRelation = relation->parent;
        if (!RelationIsValid(parentRelation)) {
            Assert(false);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not open parent relation with OID %u", relation->parentId)));
        }
        replidindex = RelationGetReplicaIndex(parentRelation);
        return replidindex;
    } else if (RelationIsPartition(relation)) {
        if (RelationIsSubPartitionOfSubPartitionTable(relation)) {
            parentRelation = RelationIdGetRelation(relation->grandparentId);
        } else {
            parentRelation = RelationIdGetRelation(relation->parentId);
        }
        if (!RelationIsValid(parentRelation)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("could not open relation with OID %u", relation->parentId)));
        }
        replidindex = RelationGetReplicaIndex(parentRelation);
        RelationClose(parentRelation);
        return replidindex;
    }
    if (relation->rd_indexvalid == 0) {
        /* RelationGetIndexList does the heavy lifting. */
        ilist = RelationGetIndexList(relation);
        list_free_ext(ilist);
        Assert(relation->rd_indexvalid != 0);
    }

    return relation->rd_replidindex;
}

/*
 * RelationGetIndexExpressions -- get the index expressions for an index
 *
 * We cache the result of transforming pg_index.indexprs into a node tree.
 * If the rel is not an index or has no expressional columns, we return NIL.
 * Otherwise, the returned tree is copied into the caller's memory context.
 * (We don't want to return a pointer to the relcache copy, since it could
 * disappear due to relcache invalidation.)
 */
List* RelationGetIndexExpressions(Relation relation)
{
    List* result = NULL;
    Datum exprsDatum;
    bool isnull = false;
    char* exprsString = NULL;
    MemoryContext oldcxt;

    /* Quick exit if we already computed the result. */
    if (relation->rd_indexprs)
        return (List*)copyObject(relation->rd_indexprs);

    /* Quick exit if there is nothing to do. */
    if (relation->rd_indextuple == NULL || heap_attisnull(relation->rd_indextuple, Anum_pg_index_indexprs, NULL))
        return NIL;

    /*
     * We build the tree we intend to return in the caller's context. After
     * successfully completing the work, we copy it into the relcache entry.
     * This avoids problems if we get some sort of error partway through.
     */
    exprsDatum = heap_getattr(relation->rd_indextuple, Anum_pg_index_indexprs,
        GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    exprsString = TextDatumGetCString(exprsDatum);
    result = (List*)stringToNode(exprsString);
    pfree_ext(exprsString);

    /*
     * Run the expressions through eval_const_expressions. This is not just an
     * optimization, but is necessary, because the planner will be comparing
     * them to similarly-processed qual clauses, and may fail to detect valid
     * matches without this.  We don't bother with canonicalize_qual, however.
     */
    result = (List*)eval_const_expressions(NULL, (Node*)result);

    /* May as well fix opfuncids too */
    fix_opfuncids((Node*)result);

    /* Now save a copy of the completed tree in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(relation->rd_indexcxt);
    relation->rd_indexprs = (List*)copyObject(result);
    (void)MemoryContextSwitchTo(oldcxt);

    return result;
}

/*
 * RelationGetDummyIndexExpressions -- get dummy expressions for an index
 *
 * Return a list of dummy expressions (just Const nodes) with the same
 * types/typmods/collations as the index's real expressions.  This is
 * useful in situations where we don't want to run any user-defined code.
 */
List* RelationGetDummyIndexExpressions(Relation relation)
{
    List* result;
    Datum exprsDatum;
    bool isnull;
    char* exprsString;
    List* rawExprs;
    ListCell* lc;

    /* Quick exit if there is nothing to do. */
    if (relation->rd_indextuple == NULL || heap_attisnull(relation->rd_indextuple, Anum_pg_index_indexprs, NULL)) {
        return NIL;
    }

    /* Extract raw node tree(s) from index tuple. */
    exprsDatum = heap_getattr(relation->rd_indextuple, Anum_pg_index_indexprs,
        GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    exprsString = TextDatumGetCString(exprsDatum);
    rawExprs = (List*)stringToNode(exprsString);
    pfree(exprsString);

    /* Construct null Consts; the typlen and typbyval are arbitrary. */
    result = NIL;
    foreach (lc, rawExprs) {
        Node* rawExpr = (Node*)lfirst(lc);
        result = lappend(
            result, makeConst(exprType(rawExpr), exprTypmod(rawExpr), exprCollation(rawExpr), 1, (Datum)0, true, true));
    }

    return result;
}

/*
 * RelationGetIndexPredicate -- get the index predicate for an index
 *
 * We cache the result of transforming pg_index.indpred into an implicit-AND
 * node tree (suitable for ExecQual).
 * If the rel is not an index or has no predicate, we return NIL.
 * Otherwise, the returned tree is copied into the caller's memory context.
 * (We don't want to return a pointer to the relcache copy, since it could
 * disappear due to relcache invalidation.)
 */
List* RelationGetIndexPredicate(Relation relation)
{
    List* result = NULL;
    Datum predDatum;
    bool isnull = false;
    char* predString = NULL;
    MemoryContext oldcxt;

    /* Quick exit if we already computed the result. */
    if (relation->rd_indpred)
        return (List*)copyObject(relation->rd_indpred);

    /* Quick exit if there is nothing to do. */
    if (relation->rd_indextuple == NULL || heap_attisnull(relation->rd_indextuple, Anum_pg_index_indpred, NULL))
        return NIL;

    /*
     * We build the tree we intend to return in the caller's context. After
     * successfully completing the work, we copy it into the relcache entry.
     * This avoids problems if we get some sort of error partway through.
     */
    predDatum = heap_getattr(relation->rd_indextuple, Anum_pg_index_indpred, GetLSCPgIndexDescriptor(), &isnull);
    Assert(!isnull);
    predString = TextDatumGetCString(predDatum);
    result = (List*)stringToNode(predString);
    pfree_ext(predString);

    /*
     * Run the expression through const-simplification and canonicalization.
     * This is not just an optimization, but is necessary, because the planner
     * will be comparing it to similarly-processed qual clauses, and may fail
     * to detect valid matches without this.  This must match the processing
     * done to qual clauses in preprocess_expression()!  (We can skip the
     * stuff involving subqueries, however, since we don't allow any in index
     * predicates.)
     */
    result = (List*)eval_const_expressions(NULL, (Node*)result);

    result = (List*)canonicalize_qual((Expr*)result, false);

    /* Also convert to implicit-AND format */
    result = make_ands_implicit((Expr*)result);

    /* May as well fix opfuncids too */
    fix_opfuncids((Node*)result);

    /* Now save a copy of the completed tree in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(relation->rd_indexcxt);
    relation->rd_indpred = (List*)copyObject(result);
    (void)MemoryContextSwitchTo(oldcxt);

    return result;
}

/*
 * Load any check constraints for the relation.
 */
static void ClusterConstraintFetch(__inout Relation relation)
{
    AttrNumber** pClusterKeys = &relation->rd_att->constr->clusterKeys;

    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData skey[1];
    HeapTuple htup;
    Datum val;
    bool isnull = false;

    ScanKeyInit(&skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(relation)));

    conrel = heap_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, skey);

    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        Form_pg_constraint conform = (Form_pg_constraint)GETSTRUCT(htup);

        /* We want check constraints only */
        if (conform->contype != CONSTRAINT_CLUSTER)
            continue;

        /* Grab and test conbin is actually set */
        val = fastgetattr(htup, Anum_pg_constraint_conkey, conrel->rd_att, &isnull);
        if (isnull)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("null cluster key for rel %s", RelationGetRelationName(relation))));

        ArrayType* arr = DatumGetArrayTypeP(val);
        int numkeys = ARR_DIMS(arr)[0];
        errno_t rc = EOK;

        *pClusterKeys = (AttrNumber*)MemoryContextAllocZero(LocalMyDBCacheMemCxt(), numkeys * sizeof(AttrNumber));

        rc = memcpy_s(*pClusterKeys, numkeys * sizeof(int16), ARR_DATA_PTR(arr), numkeys * sizeof(int16));
        securec_check(rc, "\0", "\0");

        relation->rd_att->constr->clusterKeyNum = numkeys;

        /* always only have one */
        break;
    }

    systable_endscan(conscan);
    heap_close(conrel, AccessShareLock);
}

/*
 * RelationGetIndexAttrBitmap -- get a bitmap of index attribute numbers
 *
 * The result has a bit set for each attribute used anywhere in the index
 * definitions of all the indexes on this relation.  (This includes not only
 * simple index keys, but attributes used in expressions and partial-index
 * predicates.)
 *
 * Depending on attrKind, a bitmap covering the attnums for all index columns,
 * for all key columns or for all the columns the configured replica identity
 * are returned.
 *
 * Attribute numbers are offset by FirstLowInvalidHeapAttributeNumber so that
 * we can include system attributes (e.g., OID) in the bitmap representation.
 *
 * Caller had better hold at least RowExclusiveLock on the target relation
 * to ensure that it has a stable set of indexes.  This also makes it safe
 * (deadlock-free) for us to take locks on the relation's indexes.
 *
 * The returned result is palloc'd in the caller's memory context and should
 * be bms_free'd when not needed anymore.
 */
Bitmapset* RelationGetIndexAttrBitmap(Relation relation, IndexAttrBitmapKind attrKind)
{
    Bitmapset* indexattrs = NULL;
    Bitmapset* uindexattrs = NULL;
    List* indexoidlist = NULL;
    ListCell* l = NULL;
    Bitmapset  *pkindexattrs;       /* columns in the primary index */
    Bitmapset* idindexattrs = NULL; /* columns in the the replica identity */
    Oid relpkindex;
    Oid relreplindex;
    MemoryContext oldcxt;

    Assert(!RelationIsBucket(relation) && !RelationIsPartition(relation));
    /* Quick exit if we already computed the result. */
    if (relation->rd_indexattr != NULL) {
        switch (attrKind) {
            case INDEX_ATTR_BITMAP_ALL:
                return bms_copy(relation->rd_indexattr);
            case INDEX_ATTR_BITMAP_KEY:
                return bms_copy(relation->rd_keyattr);
            case INDEX_ATTR_BITMAP_PRIMARY_KEY:
                return bms_copy(relation->rd_pkattr);
            case INDEX_ATTR_BITMAP_IDENTITY_KEY:
                return bms_copy(relation->rd_idattr);
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown attrKind %u", attrKind)));
        }
    }

    /* Fast path if definitely no indexes */
    if (!RelationGetForm(relation)->relhasindex)
        return NULL;

    /*
     * Get cached list of index OIDs
     */
    indexoidlist = RelationGetIndexList(relation);

    /* Fall out if no indexes (but relhasindex was set) */
    if (indexoidlist == NIL)
        return NULL;

    /*
     * Copy the rd_pkindex and rd_replidindex value computed by
     * RelationGetIndexList before proceeding.  This is needed because a
     * relcache flush could occur inside index_open below, resetting the
     * fields managed by RelationGetIndexList. (The values we're computing
     * will still be valid, assuming that caller has a sufficient lock on
     * the relation.)
    */
    relpkindex = relation->rd_pkindex;
    relreplindex = relation->rd_replidindex;

    /*
     * For each index, add referenced attributes to indexattrs.
     *
     * Note: we consider all indexes returned by RelationGetIndexList, even if
     * they are not indisready or indisvalid.  This is important because an
     * index for which CREATE INDEX CONCURRENTLY has just started must be
     * included in HOT-safety decisions (see README.HOT).  If a DROP INDEX
     * CONCURRENTLY is far enough along that we should ignore the index, it
     * won't be returned at all by RelationGetIndexList.
     */
    indexattrs = NULL;
    uindexattrs = NULL;
    pkindexattrs = NULL;
    idindexattrs = NULL;
    foreach (l, indexoidlist) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        IndexInfo* indexInfo = NULL;
        int i;
        bool isKey = false;    /* candidate key */
        bool isPK;		      /* primary key */
        bool isIDKey = false; /* replica identity index */

        indexDesc = index_open(indexOid, AccessShareLock);

        /* Extract index key information from the index's pg_index row */
        indexInfo = BuildIndexInfo(indexDesc);

        /* Can this index be referenced by a foreign key? */
        isKey = indexInfo->ii_Unique && indexInfo->ii_Expressions == NIL && indexInfo->ii_Predicate == NIL;
        /* Is this a primary key? */
        isPK = (indexOid == relpkindex);

        /* Is this index the configured (or default) replica identity? */
        isIDKey = (indexOid == relreplindex);

        /* Collect simple attribute references */
        for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
            int attrnum = indexInfo->ii_KeyAttrNumbers[i];

            /*
             * Since we have covering indexes with non-key columns, we must
             * handle them accurately here. non-key columns must be added into
             * indexattrs, since they are in index, and HOT-update shouldn't
             * miss them. Obviously, non-key columns couldn't be referenced by
             * foreign key or identity key. Hence we do not include them into
             * uindexattrs, pkindexattrs and idindexattrs bitmaps.
             */
            if (attrnum != 0) {
                indexattrs = bms_add_member(indexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);
                if (isKey && i < indexInfo->ii_NumIndexKeyAttrs) {
                    uindexattrs = bms_add_member(uindexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);
                }
                if (isIDKey && i < indexInfo->ii_NumIndexKeyAttrs)
                    idindexattrs = bms_add_member(idindexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);

                if (isPK) {
                    pkindexattrs = bms_add_member(pkindexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);
                }
            }
        }

        /* Collect all attributes used in expressions, too */
        pull_varattnos((Node*)indexInfo->ii_Expressions, 1, &indexattrs);

        /* Collect all attributes in the index predicate, too */
        pull_varattnos((Node*)indexInfo->ii_Predicate, 1, &indexattrs);

        index_close(indexDesc, AccessShareLock);
    }

    list_free_ext(indexoidlist);

    /* Don't leak the old values of these bitmaps, if any */
    bms_free(relation->rd_indexattr);
    relation->rd_indexattr = NULL;
    bms_free(relation->rd_pkattr);
    relation->rd_pkattr = NULL;
    bms_free(relation->rd_idattr);
    relation->rd_idattr = NULL;

    /*
     * Now save copies of the bitmaps in the relcache entry.  We intentionally
     * set rd_indexattr last, because that's the one that signals validity of
     * the values; if we run out of memory before making that copy, we won't
     * leave the relcache entry looking like the other ones are valid but
     * empty.
     */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation->rd_keyattr = bms_copy(uindexattrs);
    relation->rd_pkattr = bms_copy(pkindexattrs);
    relation->rd_idattr = bms_copy(idindexattrs);
    relation->rd_indexattr = bms_copy(indexattrs);
    (void)MemoryContextSwitchTo(oldcxt);

    /* We return our original working copy for caller to play with */
    switch (attrKind) {
        case INDEX_ATTR_BITMAP_ALL:
            return indexattrs;
        case INDEX_ATTR_BITMAP_KEY:
            return uindexattrs;
        case INDEX_ATTR_BITMAP_PRIMARY_KEY:
            return bms_copy(relation->rd_pkattr);
        case INDEX_ATTR_BITMAP_IDENTITY_KEY:
            return idindexattrs;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown attrKind %u", attrKind)));
    }
    return NULL;  // just for clear compile warning
}

/*
 * IndexGetAttrBitmap -- get a bitmap of the given index's attribute columns
 *
 * The result has a bit set for each attribute used in the index.
 *
 * Caller had better hold at least RowShareLock on the index to ensure that
 * the index columns won't be changed by DDL statements.
 *
 * The returned result is palloc'd in the caller's memory context and should
 * be bms_free'd when not needed anymore.
 */
Bitmapset* IndexGetAttrBitmap(Relation relation, IndexInfo *indexInfo)
{
    Bitmapset* indexattrs = NULL;
    MemoryContext oldcxt;

    /* make sure relation is a index relation */
    Assert(relation->rd_rel->relam != 0);

    /*
     * Quick exit if we already computed the result.
     *      Note: this field is shared with heap relation.
     */
    if (relation->rd_indexattr != NULL) {
        return bms_copy(relation->rd_indexattr);
    }

    /* Collect simple attribute references */
    for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
        int attrnum = indexInfo->ii_KeyAttrNumbers[i];
        /*
         * Since we have covering indexes with non-key columns, we must
         * handle them accurately here. non-key columns must be added into
         * indexattrs, since they are in index, and HOT-update shouldn't
         * miss them.
         */
        if (attrnum != 0) {
            indexattrs = bms_add_member(indexattrs, attrnum - FirstLowInvalidHeapAttributeNumber);
        }
    }

    /* Collect all attributes used in expressions, too */
    pull_varattnos((Node*)indexInfo->ii_Expressions, 1, &indexattrs);

    /* Collect all attributes in the index predicate, too */
    pull_varattnos((Node*)indexInfo->ii_Predicate, 1, &indexattrs);

    /* Now save copies of the bitmaps in the relcache entry */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation->rd_indexattr = bms_copy(indexattrs);
    (void)MemoryContextSwitchTo(oldcxt);

    return indexattrs;
}

/*
 * RelationGetExclusionInfo -- get info about index's exclusion constraint
 *
 * This should be called only for an index that is known to have an
 * associated exclusion constraint.  It returns arrays (palloc'd in caller's
 * context) of the exclusion operator OIDs, their underlying functions'
 * OIDs, and their strategy numbers in the index's opclasses.  We cache
 * all this information since it requires a fair amount of work to get.
 */
void RelationGetExclusionInfo(Relation indexRelation, Oid** operators, Oid** procs, uint16** strategies)
{
    int indnkeyatts;
    Oid* ops = NULL;
    Oid* funcs = NULL;
    uint16* strats = NULL;
    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData skey[1];
    HeapTuple htup;
    bool found = false;
    MemoryContext oldcxt;
    int i;

    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(indexRelation);

    /* Allocate result space in caller context */
    *operators = ops = (Oid*)palloc(sizeof(Oid) * indnkeyatts);
    *procs = funcs = (Oid*)palloc(sizeof(Oid) * indnkeyatts);
    *strategies = strats = (uint16*)palloc(sizeof(uint16) * indnkeyatts);

    /* Quick exit if we have the data cached already */
    if (indexRelation->rd_exclstrats != NULL) {
        errno_t rc = memcpy_s(ops, sizeof(Oid) * indnkeyatts, indexRelation->rd_exclops, sizeof(Oid) * indnkeyatts);
        securec_check(rc, "", "");
        rc = memcpy_s(funcs, sizeof(Oid) * indnkeyatts, indexRelation->rd_exclprocs, sizeof(Oid) * indnkeyatts);
        securec_check(rc, "", "");
        rc = memcpy_s(strats, sizeof(uint16) * indnkeyatts, indexRelation->rd_exclstrats, sizeof(uint16) * indnkeyatts);
        securec_check(rc, "", "");
        return;
    }

    /*
     * Search pg_constraint for the constraint associated with the index. To
     * make this not too painfully slow, we use the index on conrelid; that
     * will hold the parent relation's OID not the index's own OID.
     */
    ScanKeyInit(&skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(indexRelation->rd_index->indrelid));

    conrel = heap_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidIndexId, true, NULL, 1, skey);
    found = false;

    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        Form_pg_constraint conform = (Form_pg_constraint)GETSTRUCT(htup);
        Datum val;
        bool isnull = false;
        ArrayType* arr = NULL;
        int nelem;

        /* We want the exclusion constraint owning the index */
        if (conform->contype != CONSTRAINT_EXCLUSION || conform->conindid != RelationGetRelid(indexRelation))
            continue;

        /* There should be only one */
        if (found)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("unexpected exclusion constraint record found for rel %s",
                        RelationGetRelationName(indexRelation))));
        found = true;

        /* Extract the operator OIDS from conexclop */
        val = fastgetattr(htup, Anum_pg_constraint_conexclop, conrel->rd_att, &isnull);
        if (isnull)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("null conexclop for rel %s", RelationGetRelationName(indexRelation))));

        arr = DatumGetArrayTypeP(val); /* ensure not toasted */
        nelem = ARR_DIMS(arr)[0];
        if (ARR_NDIM(arr) != 1 || nelem != indnkeyatts || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID)
            ereport(
                ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("conexclop is not a 1-D Oid array")));

        int rc = memcpy_s(ops, sizeof(Oid) * indnkeyatts, ARR_DATA_PTR(arr), sizeof(Oid) * indnkeyatts);
        securec_check(rc, "\0", "\0");
    }

    systable_endscan(conscan);
    heap_close(conrel, AccessShareLock);

    if (!found)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("exclusion constraint record missing for rel %s", RelationGetRelationName(indexRelation))));

    /* We need the func OIDs and strategy numbers too */
    for (i = 0; i < indnkeyatts; i++) {
        funcs[i] = get_opcode(ops[i]);
        strats[i] = get_op_opfamily_strategy(ops[i], indexRelation->rd_opfamily[i]);
        /* shouldn't fail, since it was checked at index creation */
        if (strats[i] == InvalidStrategy)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("could not find strategy for operator %u in family %u",
                        ops[i],
                        indexRelation->rd_opfamily[i])));
    }

    /* Save a copy of the results in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(indexRelation->rd_indexcxt);

    indexRelation->rd_exclops = (Oid*)palloc(sizeof(Oid) * indnkeyatts);
    indexRelation->rd_exclprocs = (Oid*)palloc(sizeof(Oid) * indnkeyatts);
    indexRelation->rd_exclstrats = (uint16*)palloc(sizeof(uint16) * indnkeyatts);

    errno_t rc = memcpy_s(indexRelation->rd_exclops, sizeof(Oid) * indnkeyatts, ops, sizeof(Oid) * indnkeyatts);
    securec_check(rc, "", "");
    rc = memcpy_s(indexRelation->rd_exclprocs, sizeof(Oid) * indnkeyatts, funcs, sizeof(Oid) * indnkeyatts);
    securec_check(rc, "", "");
    rc = memcpy_s(indexRelation->rd_exclstrats, sizeof(uint16) * indnkeyatts, strats, sizeof(uint16) * indnkeyatts);
    securec_check(rc, "", "");
    (void)MemoryContextSwitchTo(oldcxt);
}

/*
 * Get publication actions for the given relation.
 */
struct PublicationActions* GetRelationPublicationActions(Relation relation)
{
    List* puboids;
    ListCell* lc;
    MemoryContext oldcxt;
    int rc;
    PublicationActions* pubactions = (PublicationActions*)palloc0(sizeof(PublicationActions));

    /*
     * If not publishable, it publishes no actions.  (pgoutput_change() will
     * ignore it.)
     */
    if (!is_publishable_relation(relation)) {
        return pubactions;
    }

    if (relation->rd_pubactions) {
        errno_t rcs = memcpy_s(pubactions, sizeof(PublicationActions),
            relation->rd_pubactions, sizeof(PublicationActions));
        securec_check(rcs, "\0", "\0");
        return pubactions;
    }

    /* Fetch the publication membership info. */
    puboids = GetRelationPublications(RelationGetRelid(relation));
    puboids = list_concat_unique_oid(puboids, GetAllTablesPublications());

    foreach(lc, puboids)
    {
        Oid pubid = lfirst_oid(lc);
        HeapTuple tup;
        Form_pg_publication pubform;

        tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));

        if (!HeapTupleIsValid(tup))
            elog(ERROR, "cache lookup failed for publication %u", pubid);

        pubform = (Form_pg_publication) GETSTRUCT(tup);

        pubactions->pubinsert |= pubform->pubinsert;
        pubactions->pubupdate |= pubform->pubupdate;
        pubactions->pubdelete |= pubform->pubdelete;

        ReleaseSysCache(tup);

        /*
         * If we know everything is replicated, there is no point to check
         * for other publications.
         */
        if (pubactions->pubinsert && pubactions->pubupdate && pubactions->pubdelete)
            break;
    }

    if (relation->rd_pubactions) {
        pfree(relation->rd_pubactions);
        relation->rd_pubactions = NULL;
    }

    /* Now save copy of the actions in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    relation->rd_pubactions = palloc(sizeof(PublicationActions));
    rc = memcpy_s(relation->rd_pubactions, sizeof(PublicationActions), pubactions, sizeof(PublicationActions));
    securec_check(rc, "", "");
    MemoryContextSwitchTo(oldcxt);

    return pubactions;
}

/*
 *	load_relcache_init_file, write_relcache_init_file
 *
 *		In late 1992, we started regularly having databases with more than
 *		a thousand classes in them.  With this number of classes, it became
 *		critical to do indexed lookups on the system catalogs.
 *
 *		Bootstrapping these lookups is very hard.  We want to be able to
 *		use an index on pg_attribute, for example, but in order to do so,
 *		we must have read pg_attribute for the attributes in the index,
 *		which implies that we need to use the index.
 *
 *		In order to get around the problem, we do the following:
 *
 *		   +  When the database system is initialized (at initdb time), we
 *			  don't use indexes.  We do sequential scans.
 *
 *		   +  When the backend is started up in normal mode, we load an image
 *			  of the appropriate relation descriptors, in internal format,
 *			  from an initialization file in the data/base/... directory.
 *
 *		   +  If the initialization file isn't there, then we create the
 *			  relation descriptors using sequential scans and write 'em to
 *			  the initialization file for use by subsequent backends.
 *
 *		As of Postgres 9.0, there is one local initialization file in each
 *		database, plus one shared initialization file for shared catalogs.
 *
 *		We could dispense with the initialization files and just build the
 *		critical reldescs the hard way on every backend startup, but that
 *		slows down backend startup noticeably.
 *
 *		We can in fact go further, and save more relcache entries than
 *		just the ones that are absolutely critical; this allows us to speed
 *		up backend startup by not having to build such entries the hard way.
 *		Presently, all the catalog and index entries that are referred to
 *		by catcaches are stored in the initialization files.
 *
 *		The same mechanism that detects when catcache and relcache entries
 *		need to be invalidated (due to catalog updates) also arranges to
 *		unlink the initialization files when the contents may be out of date.
 *		The files will then be rebuilt during the next backend startup.
 */

/*
 * load_relcache_init_file -- attempt to load cache from the shared
 * or local cache init file
 *
 * If successful, return TRUE and set u_sess->relcache_cxt.criticalRelcachesBuilt or
 * u_sess->relcache_cxt.criticalSharedRelcachesBuilt to true.
 * If not successful, return FALSE.
 *
 * NOTE: we assume we are already switched into u_sess->cache_mem_cxt.
 */
static bool load_relcache_init_file(bool shared)
{
    FILE* fp = NULL;
    char initfilename[MAXPGPATH];
    Relation* rels = NULL;
    int relno, num_rels, max_rels, nailed_rels, nailed_indexes, magic;
    int i;
    errno_t rc;

    if (shared) 
        rc = snprintf_s(initfilename,
            sizeof(initfilename),
            sizeof(initfilename) - 1,
            "global/%s.%u",
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM);
    else
        rc = snprintf_s(initfilename,
            sizeof(initfilename),
            sizeof(initfilename) - 1,
            "%s/%s.%u",
            u_sess->proc_cxt.DatabasePath,
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM);
    
    securec_check_ss(rc, "\0", "\0");

    fp = AllocateFile(initfilename, PG_BINARY_R);
    if (fp == NULL)
        return false;

    /*
     * Read the index relcache entries from the file.  Note we will not enter
     * any of them into the cache if the read fails partway through; this
     * helps to guard against broken init files.
     */
    max_rels = 100;
    rels = (Relation*)palloc(max_rels * sizeof(Relation));
    num_rels = 0;
    nailed_rels = nailed_indexes = 0;

    /* check for correct magic number (compatible version) */
    if (fread(&magic, 1, sizeof(magic), fp) != sizeof(magic))
        goto read_failed;
    if (magic != RELCACHE_INIT_FILEMAGIC)
        goto read_failed;

    for (relno = 0;; relno++) {
        Size len;
        size_t nread;
        Relation rel;
        Form_pg_class relform;
        bool has_not_null = false;
        int default_num;

        /* first read the relation descriptor length */
        nread = fread(&len, 1, sizeof(len), fp);
        if (nread != sizeof(len)) {
            if (nread == 0)
                break; /* end of file */
            goto read_failed;
        }

        /* safety check for incompatible relcache layout */
        if (len != sizeof(RelationData))
            goto read_failed;

        /* allocate another relcache header */
        if (num_rels >= max_rels) {
            max_rels *= 2;
            rels = (Relation*)repalloc(rels, max_rels * sizeof(Relation));
        }

        rel = rels[num_rels++] = (Relation)palloc(len);

        /* then, read the Relation structure */
        if (fread(rel, 1, len, fp) != len)
            goto read_failed;

        /* next read the relation tuple form */
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
            goto read_failed;

        if (len != CLASS_TUPLE_SIZE) {
            goto read_failed;
        }
        relform = (Form_pg_class)palloc(len);
        if (fread(relform, 1, len, fp) != len)
            goto read_failed;

        rel->rd_rel = relform;

        /* initialize attribute tuple forms */
        //XXTAM:
        rel->rd_att = CreateTemplateTupleDesc(relform->relnatts, relform->relhasoids, TAM_HEAP);
        rel->rd_att->tdrefcount = 1; /* mark as refcounted */

        rel->rd_att->tdtypeid = relform->reltype;
        rel->rd_att->tdtypmod = -1; /* unnecessary, but... */

        /* next read all the attribute tuple form data entries */
        has_not_null = false;
        default_num = 0;
        for (i = 0; i < relform->relnatts; i++) {
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len != ATTRIBUTE_FIXED_PART_SIZE)
                goto read_failed;
            if (fread(rel->rd_att->attrs[i], 1, len, fp) != len)
                goto read_failed;

            has_not_null = has_not_null || rel->rd_att->attrs[i]->attnotnull;

            if (rel->rd_att->attrs[i]->atthasdef) {
                /*
                 * Caution! For autovacuum, catchup and walsender thread, they will return before
                 * calling RelationCacheInitializePhase3, so they cannot load default vaules of adding
                 * new columns during inplace upgrade.
                 * Here, we do not mark those three special threads by increasing default_num, in
                 * case that inconsistent things happen when destroy or rebuild relcache.
                 */
                if (!((IsAutoVacuumLauncherProcess() || IsCatchupProcess() || AM_WAL_SENDER) && shared))
                    default_num++;
            }
        }

        /* next read the access method specific field */
        if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
            goto read_failed;
        if (len > 0 && len < MaxAllocSize) {
            rel->rd_options = (bytea*)palloc(len);
            if (fread(rel->rd_options, 1, len, fp) != len)
                goto read_failed;
            if (len != VARSIZE(rel->rd_options))
                goto read_failed; /* sanity check */
        } else {
            rel->rd_options = NULL;
        }

        if (RelationIsRedistributeDest(rel))
            rel->rd_att->tdisredistable = true;

        /* mark not-null status */
        if (has_not_null || default_num) {
            TupleConstr* constr = (TupleConstr*)palloc0(sizeof(TupleConstr));

            constr->has_not_null = has_not_null;
            constr->num_defval = default_num;
            rel->rd_att->constr = constr;
        }

        /* If it's an index, there's more to do */
        if (RelationIsIndex(rel)) {
            Form_pg_am am;
            MemoryContext indexcxt;
            Oid* opfamily = NULL;
            Oid* opcintype = NULL;
            RegProcedure* support = NULL;
            int nsupport;
            int16* indoption = NULL;
            Oid* indcollation = NULL;

            /* Count nailed indexes to ensure we have 'em all */
            if (rel->rd_isnailed)
                nailed_indexes++;

            /* next, read the pg_index tuple */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len > HEAPTUPLESIZE + MaxIndexTuplesPerPage) {
                goto read_failed;
            }
            rel->rd_indextuple = (HeapTuple)heaptup_alloc(len);
            if (fread(rel->rd_indextuple, 1, len, fp) != len)
                goto read_failed;

            /* Fix up internal pointers in the tuple -- see heap_copytuple */
            rel->rd_indextuple->t_data = (HeapTupleHeader)((char*)rel->rd_indextuple + HEAPTUPLESIZE);
            rel->rd_index = (Form_pg_index)GETSTRUCT(rel->rd_indextuple);
            IndexRelationInitKeyNums(rel);

            /* next, read the access method tuple form */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;

            if (len != sizeof(FormData_pg_am)) {
                goto read_failed;
            }
            am = (Form_pg_am)palloc(len);
            if (fread(am, 1, len, fp) != len)
                goto read_failed;
            rel->rd_am = am;

            /*
             * prepare index info context --- parameters should match
             * RelationInitIndexAccessInfo
             */
            indexcxt = AllocSetContextCreate(u_sess->cache_mem_cxt,
                RelationGetRelationName(rel),
                ALLOCSET_SMALL_MINSIZE,
                ALLOCSET_SMALL_INITSIZE,
                ALLOCSET_SMALL_MAXSIZE);
            rel->rd_indexcxt = indexcxt;

            /* next, read the vector of opfamily OIDs */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len > relform->relnatts * sizeof(Oid)) {
                goto read_failed;
            }
            opfamily = (Oid*)MemoryContextAlloc(indexcxt, len);
            if (fread(opfamily, 1, len, fp) != len)
                goto read_failed;

            rel->rd_opfamily = opfamily;

            /* next, read the vector of opcintype OIDs */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;

            if (len > relform->relnatts * sizeof(Oid)) {
                goto read_failed;
            }
            opcintype = (Oid*)MemoryContextAlloc(indexcxt, len);
            if (fread(opcintype, 1, len, fp) != len)
                goto read_failed;

            rel->rd_opcintype = opcintype;

            /* next, read the vector of support procedure OIDs */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len > relform->relnatts * (am->amsupport * sizeof(RegProcedure))) {
                goto read_failed;
            }
            support = (RegProcedure*)MemoryContextAlloc(indexcxt, len);
            if (fread(support, 1, len, fp) != len)
                goto read_failed;

            rel->rd_support = support;

            /* next, read the vector of collation OIDs */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len > relform->relnatts * sizeof(Oid)) {
                goto read_failed;
            }
            indcollation = (Oid*)MemoryContextAlloc(indexcxt, len);
            if (fread(indcollation, 1, len, fp) != len)
                goto read_failed;

            rel->rd_indcollation = indcollation;

            /* finally, read the vector of indoption values */
            if (fread(&len, 1, sizeof(len), fp) != sizeof(len))
                goto read_failed;
            if (len > relform->relnatts * sizeof(int16)) {
                goto read_failed;
            }
            indoption = (int16*)MemoryContextAlloc(indexcxt, len);
            if (fread(indoption, 1, len, fp) != len)
                goto read_failed;

            rel->rd_indoption = indoption;

            /* set up zeroed fmgr-info vectors */
            rel->rd_aminfo = (RelationAmInfo*)MemoryContextAllocZero(indexcxt, sizeof(RelationAmInfo));
            nsupport = relform->relnatts * am->amsupport;
            rel->rd_supportinfo = (FmgrInfo*)MemoryContextAllocZero(indexcxt, nsupport * sizeof(FmgrInfo));
        } else {
            /* Count nailed rels to ensure we have 'em all */
            if (rel->rd_isnailed)
                nailed_rels++;

            Assert(rel->rd_index == NULL);
            Assert(rel->rd_indextuple == NULL);
            Assert(rel->rd_am == NULL);
            Assert(rel->rd_indexcxt == NULL);
            Assert(rel->rd_aminfo == NULL);
            Assert(rel->rd_opfamily == NULL);
            Assert(rel->rd_opcintype == NULL);
            Assert(rel->rd_support == NULL);
            Assert(rel->rd_supportinfo == NULL);
            Assert(rel->rd_indoption == NULL);
            Assert(rel->rd_indcollation == NULL);
        }

        /*
         * Rules and triggers are not saved (mainly because the internal
         * format is complex and subject to change).  They must be rebuilt if
         * needed by RelationCacheInitializePhase3.  This is not expected to
         * be a big performance hit since few system catalogs have such. Ditto
         * for index expressions, predicates, exclusion info, and FDW info.
         */
        rel->rd_rules = NULL;
        rel->rd_rulescxt = NULL;
        rel->trigdesc = NULL;
        rel->rd_indexprs = NIL;
        rel->rd_rlsdesc = NULL;
        rel->rd_indpred = NIL;
        rel->rd_exclops = NULL;
        rel->rd_exclprocs = NULL;
        rel->rd_exclstrats = NULL;
        rel->rd_fdwroutine = NULL;

        /*
         * Reset transient-state fields in the relcache entry
         */
        rel->rd_smgr = NULL;
        rel->rd_bucketkey = NULL;
        rel->rd_bucketoid = InvalidOid;		
        if (rel->rd_isnailed)
            rel->rd_refcnt = 1;
        else
            rel->rd_refcnt = 0;
        rel->rd_indexvalid = 0;
        rel->rd_indexlist = NIL;
        rel->rd_oidindex = InvalidOid;
        rel->rd_pkindex = InvalidOid;
        rel->rd_indexattr = NULL;
        rel->rd_keyattr = NULL;
        rel->rd_pkattr = NULL;
        rel->rd_idattr = NULL;
        rel->rd_pubactions = NULL;
        rel->rd_createSubid = InvalidSubTransactionId;
        rel->rd_newRelfilenodeSubid = InvalidSubTransactionId;
        rel->rd_amcache = NULL;
        rel->rd_rootcache = InvalidBuffer;
        rel->pgstat_info = NULL;

        /*
         * Recompute lock and physical addressing info.  This is needed in
         * case the pg_internal.init file was copied from some other database
         * by CREATE DATABASE.
         */
        RelationInitLockInfo(rel);
        RelationInitPhysicalAddr(rel);
        if (rel->rd_rel->relkind == RELKIND_MATVIEW && heap_is_matview_init_state(rel))
            rel->rd_isscannable = false;
        else
            rel->rd_isscannable = true;
    }

    /*
     * We reached the end of the init file without apparent problem. Did we
     * get the right number of nailed items?  (This is a useful crosscheck in
     * case the set of critical rels or indexes changes.)
     */
    if (shared) {
        if (nailed_rels != NUM_CRITICAL_SHARED_RELS || nailed_indexes != NUM_CRITICAL_SHARED_INDEXES)
            goto read_failed;
    } else {
        if (nailed_rels != NUM_CRITICAL_LOCAL_RELS || nailed_indexes != NUM_CRITICAL_LOCAL_INDEXES)
            goto read_failed;
    }

    /*
     * OK, all appears well.
     *
     * Now insert all the new relcache entries into the cache.
     */
    for (relno = 0; relno < num_rels; relno++) {
        RelationIdCacheInsertIntoLocal(rels[relno]);
        /* also make a list of their OIDs, for RelationIdIsInInitFile */
        if (!shared)
            u_sess->relcache_cxt.initFileRelationIds =
                lcons_oid(RelationGetRelid(rels[relno]), u_sess->relcache_cxt.initFileRelationIds);
    }

    pfree_ext(rels);
    FreeFile(fp);

    if (shared)
        u_sess->relcache_cxt.criticalSharedRelcachesBuilt = true;
    else
        u_sess->relcache_cxt.criticalRelcachesBuilt = true;
    return true;

    /*
     * init file is broken, so do it the hard way.	We don't bother trying to
     * free the clutter we just allocated; it's not in the relcache so it
     * won't hurt.
     */
read_failed:
    pfree_ext(rels);
    FreeFile(fp);

    return false;
}

/*
 * Write out a new initialization file with the current contents
 * of the relcache (either shared rels or local rels, as indicated).
 */
static void write_relcache_init_file(bool shared)
{
    FILE* fp = NULL;
    char tempfilename[MAXPGPATH];
    char finalfilename[MAXPGPATH];
    int magic;
    HASH_SEQ_STATUS status;
    RelIdCacheEnt* idhentry = NULL;
    MemoryContext oldcxt;
    int i;
    errno_t rc;

    /*
     * We must write a temporary file and rename it into place. Otherwise,
     * another backend starting at about the same time might crash trying to
     * read the partially-complete file.
     *
     * During inplace or online upgrade, for the first backend that we
     * launch after we replace new-version gaussdb binary, it would write
     * a special init file, named as xxx.old_version_num.upgrade, with
     * all old-version catalog schemas.
     * Consequent backends launched during upgrade rely on this specific
     * init file to build catalog relcache.
     * Then, after upgrade ends, any new backends turn to an init file
     * named as xxx.new_version_num
     */
    if (shared) {
        rc = snprintf_s(tempfilename,
            sizeof(tempfilename),
            sizeof(tempfilename) - 1,
            "global/%s.%u.%lu",
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM,
            t_thrd.proc_cxt.MyProcPid);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(finalfilename,
            sizeof(finalfilename),
            sizeof(finalfilename) - 1,
            "global/%s.%u",
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(tempfilename,
            sizeof(tempfilename),
            sizeof(tempfilename) - 1,
            "%s/%s.%u.%lu",
            u_sess->proc_cxt.DatabasePath,
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM,
            t_thrd.proc_cxt.MyProcPid);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(finalfilename,
            sizeof(finalfilename),
            sizeof(finalfilename) - 1,
            "%s/%s.%u",
            u_sess->proc_cxt.DatabasePath,
            RELCACHE_INIT_FILENAME,
            GRAND_VERSION_NUM);
        securec_check_ss(rc, "\0", "\0");
    }

    unlink(tempfilename); /* in case it exists w/wrong permissions */

    fp = AllocateFile(tempfilename, PG_BINARY_W);
    if (fp == NULL) {
        /*
         * We used to consider this a fatal error, but we might as well
         * continue with backend startup ...
         */
        ereport(WARNING,
            (errcode_for_file_access(),
                errmsg("could not create relation-cache initialization file \"%s\": %m", tempfilename),
                errdetail("Continuing anyway, but there's something wrong.")));
        return;
    }

    /*
     * Write a magic number to serve as a file version identifier.	We can
     * change the magic number whenever the relcache layout changes.
     */
    magic = RELCACHE_INIT_FILEMAGIC;
    if (fwrite(&magic, 1, sizeof(magic), fp) != sizeof(magic))
        ereport(FATAL, (errmsg("could not write init file")));

    /*
     * Write all the appropriate reldescs (in no particular order).
     */
    hash_seq_init(&status, u_sess->relcache_cxt.RelationIdCache);

    while ((idhentry = (RelIdCacheEnt*)hash_seq_search(&status)) != NULL) {
        Relation rel = idhentry->reldesc;
        Form_pg_class relform = rel->rd_rel;

        /* ignore if not correct group */
        if (relform->relisshared != shared)
            continue;

        /* first write the relcache entry proper */
        write_item(rel, sizeof(RelationData), fp);

        /* next write the relation tuple form */
        write_item(relform, CLASS_TUPLE_SIZE, fp);

        /* next, do all the attribute tuple form data entries */
        for (i = 0; i < relform->relnatts; i++) {
            write_item(rel->rd_att->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, fp);
        }

        /* next, do the access method specific field */
        write_item(rel->rd_options, (rel->rd_options ? VARSIZE(rel->rd_options) : 0), fp);

        /* If it's an index, there's more to do */
        if (RelationIsIndex(rel)) {
            Form_pg_am am = rel->rd_am;

            /* write the pg_index tuple */
            /* we assume this was created by heap_copytuple! */
            write_item(rel->rd_indextuple, HEAPTUPLESIZE + rel->rd_indextuple->t_len, fp);

            /* next, write the access method tuple form */
            write_item(am, sizeof(FormData_pg_am), fp);

            /* next, write the vector of opfamily OIDs */
            write_item(rel->rd_opfamily, relform->relnatts * sizeof(Oid), fp);

            /* next, write the vector of opcintype OIDs */
            write_item(rel->rd_opcintype, relform->relnatts * sizeof(Oid), fp);

            /* next, write the vector of support procedure OIDs */
            write_item(rel->rd_support, relform->relnatts * (am->amsupport * sizeof(RegProcedure)), fp);

            /* next, write the vector of collation OIDs */
            write_item(rel->rd_indcollation, relform->relnatts * sizeof(Oid), fp);

            /* finally, write the vector of indoption values */
            write_item(rel->rd_indoption, relform->relnatts * sizeof(int16), fp);
        }

        /* also make a list of their OIDs, for RelationIdIsInInitFile */
        if (!shared) {
            oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
            u_sess->relcache_cxt.initFileRelationIds =
                lcons_oid(RelationGetRelid(rel), u_sess->relcache_cxt.initFileRelationIds);
            (void)MemoryContextSwitchTo(oldcxt);
        }
    }

    if (FreeFile(fp))
        ereport(FATAL, (errmsg("could not write init file")));

    /*
     * Now we have to check whether the data we've so painstakingly
     * accumulated is already obsolete due to someone else's just-committed
     * catalog changes.  If so, we just delete the temp file and leave it to
     * the next backend to try again.  (Our own relcache entries will be
     * updated by SI message processing, but we can't be sure whether what we
     * wrote out was up-to-date.)
     *
     * This mustn't run concurrently with the code that unlinks an init file
     * and sends SI messages, so grab a serialization lock for the duration.
     */
    LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);

    /* Make sure we have seen all incoming SI messages */
    AcceptInvalidationMessages();

    /*
     * If we have received any SI relcache invals since backend start, assume
     * we may have written out-of-date data.
     */
    if (u_sess->relcache_cxt.relcacheInvalsReceived == 0L) {
        /*
         * OK, rename the temp file to its final name, deleting any
         * previously-existing init file.
         *
         * Note: a failure here is possible under Cygwin, if some other
         * backend is holding open an unlinked-but-not-yet-gone init file. So
         * treat this as a noncritical failure; just remove the useless temp
         * file on failure.
         */
        if (rename(tempfilename, finalfilename) < 0)
            unlink(tempfilename);
    } else {
        /* Delete the already-obsolete temp file */
        unlink(tempfilename);
    }

    LWLockRelease(RelCacheInitLock);
}

/* write a chunk of data preceded by its length */
static void write_item(const void* data, Size len, FILE* fp)
{
    if (fwrite(&len, 1, sizeof(len), fp) != sizeof(len))
        ereport(FATAL, (errmsg("could not write init file")));
    if (fwrite(data, 1, len, fp) != len)
        ereport(FATAL, (errmsg("could not write init file")));
}

/*
 * Detect whether a given relation (identified by OID) is one of the ones
 * we store in the local relcache init file.
 *
 * Note that we effectively assume that all backends running in a database
 * would choose to store the same set of relations in the init file;
 * otherwise there are cases where we'd fail to detect the need for an init
 * file invalidation.  This does not seem likely to be a problem in practice.
 */
bool RelationIdIsInInitFile(Oid relationId)
{
    return list_member_oid(LocalRelCacheInitFileRelationIds(), relationId);
}

/*
 * Invalidate (remove) the init file during commit of a transaction that
 * changed one or more of the relation cache entries that are kept in the
 * local init file.
 *
 * To be safe against concurrent inspection or rewriting of the init file,
 * we must take RelCacheInitLock, then remove the old init file, then send
 * the SI messages that include relcache inval for such relations, and then
 * release RelCacheInitLock.  This serializes the whole affair against
 * write_relcache_init_file, so that we can be sure that any other process
 * that's concurrently trying to create a new init file won't move an
 * already-stale version into place after we unlink.  Also, because we unlink
 * before sending the SI messages, a backend that's currently starting cannot
 * read the now-obsolete init file and then miss the SI messages that will
 * force it to update its relcache entries.  (This works because the backend
 * startup sequence gets into the sinval array before trying to load the init
 * file.)
 *
 * We take the lock and do the unlink in RelationCacheInitFilePreInvalidate,
 * then release the lock in RelationCacheInitFilePostInvalidate.  Caller must
 * send any pending SI messages between those calls.
 *
 * Notice this deals only with the local init file, not the shared init file.
 * The reason is that there can never be a "significant" change to the
 * relcache entry of a shared relation; the most that could happen is
 * updates of noncritical fields such as relpages/reltuples.  So, while
 * it's worth updating the shared init file from time to time, it can never
 * be invalid enough to make it necessary to remove it.
 */
void RelationCacheInitFilePreInvalidate(void)
{
    char initfilename[MAXPGPATH];
    errno_t rc;

    rc = snprintf_s(initfilename,
        sizeof(initfilename),
        sizeof(initfilename) - 1,
        "%s/%s.%u",
        u_sess->proc_cxt.DatabasePath,
        RELCACHE_INIT_FILENAME,
        GRAND_VERSION_NUM);
    securec_check_ss(rc, "\0", "\0");

    LWLockAcquire(RelCacheInitLock, LW_EXCLUSIVE);

    if (unlink(initfilename) < 0) {
        /*
         * The file might not be there if no backend has been started since
         * the last removal.  But complain about failures other than ENOENT.
         * Fortunately, it's not too late to abort the transaction if we can't
         * get rid of the would-be-obsolete init file.
         */
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove cache file \"%s\": %m", initfilename)));
    }
}

void RelationCacheInitFilePostInvalidate(void)
{
    LWLockRelease(RelCacheInitLock);
}

/*
 * Remove the init files during postmaster startup.
 *
 * We used to keep the init files across restarts, but that is unsafe in PITR
 * scenarios, and even in simple crash-recovery cases there are windows for
 * the init files to become out-of-sync with the database.	So now we just
 * remove them during startup and expect the first backend launch to rebuild
 * them.  Of course, this has to happen in each database of the cluster.
 */
void RelationCacheInitFileRemove(void)
{
    const char* tblspcdir = "pg_tblspc";
    DIR* dir = NULL;
    struct dirent* de = NULL;
    char path[MAXPGPATH];
    errno_t rc;

    /*
     * We zap the shared cache file too.  In theory it can't get out of sync
     * enough to be a problem, but in data-corruption cases, who knows ...
     */
    rc = snprintf_s(path, sizeof(path), sizeof(path) - 1, "global/%s.%u", RELCACHE_INIT_FILENAME, GRAND_VERSION_NUM);
    securec_check_ss(rc, "\0", "\0");
    unlink_initfile(path);

    /* Scan everything in the default tablespace */
    RelationCacheInitFileRemoveInDir("base");

    /* Scan the tablespace link directory to find non-default tablespaces */
    dir = AllocateDir(tblspcdir);
    if (dir == NULL) {
        ereport(LOG, (errmsg("could not open tablespace link directory \"%s\": %m", tblspcdir)));
        return;
    }

    while ((de = ReadDir(dir, tblspcdir)) != NULL) {
        if (strspn(de->d_name, "0123456789") == strlen(de->d_name)) {
            /* Scan the tablespace dir for per-database dirs */
#ifdef PGXC
            /* Postgres-XC tablespaces include node name in path */
            rc = snprintf_s(path,
                sizeof(path),
                sizeof(path) - 1,
                "%s/%s/%s_%s",
                tblspcdir,
                de->d_name,
                TABLESPACE_VERSION_DIRECTORY,
                g_instance.attr.attr_common.PGXCNodeName);
#else
            rc = snprintf_s(
                path, sizeof(path), sizeof(path) - 1, "%s/%s/%s", tblspcdir, de->d_name, TABLESPACE_VERSION_DIRECTORY);
#endif
            securec_check_ss(rc, "\0", "\0");
            RelationCacheInitFileRemoveInDir(path);
        }
    }

    FreeDir(dir);
}

/* Process one per-tablespace directory for RelationCacheInitFileRemove */
static void RelationCacheInitFileRemoveInDir(const char* tblspcpath)
{
    DIR* dir = NULL;
    struct dirent* de = NULL;
    char initfilename[MAXPGPATH];
    errno_t rc;

    /* Scan the tablespace directory to find per-database directories */
    dir = AllocateDir(tblspcpath);
    if (dir == NULL) {
        ereport(LOG, (errmsg("could not open tablespace directory \"%s\": %m", tblspcpath)));
        return;
    }

    while ((de = ReadDir(dir, tblspcpath)) != NULL) {
        if (strspn(de->d_name, "0123456789") == strlen(de->d_name)) {
            /* Try to remove the init file in each database */
            rc = snprintf_s(initfilename,
                sizeof(initfilename),
                sizeof(initfilename) - 1,
                "%s/%s/%s.%u",
                tblspcpath,
                de->d_name,
                RELCACHE_INIT_FILENAME,
                GRAND_VERSION_NUM);
            securec_check_ss(rc, "\0", "\0");
            unlink_initfile(initfilename);
        }
    }

    FreeDir(dir);
}

static void unlink_initfile(const char* initfilename)
{
    if (unlink(initfilename) < 0) {
        /* It might not be there, but log any error other than ENOENT */
        if (errno != ENOENT)
            ereport(LOG, (errmsg("could not remove cache file \"%s\": %m", initfilename)));
    }
}

List* PartitionGetPartIndexList(Partition part, bool inc_unused)
{
    Relation partrel;
    SysScanDesc indscan;
    ScanKeyData skey;
    HeapTuple parttup;
    List* result = NULL;
    Oid oidIndex;
    MemoryContext oldcxt;

    /* Quick exit if we already computed the list. */
    if (part->pd_indexvalid != 0) {
        return list_copy(part->pd_indexlist);
    }

    /*
     * We build the list we intend to return (in the caller's context) while
     * doing the scan.	After successfully completing the scan, we copy that
     * list into the relcache entry.  This avoids cache-context memory leakage
     * if we get some sort of error partway through.
     */
    result = NIL;
    oidIndex = InvalidOid;

    /* Prepare to scan pg_partition for entries having indextblid = this rel. */
    ScanKeyInit(&skey,
        Anum_pg_partition_indextblid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(PartitionGetPartid(part)));

    partrel = heap_open(PartitionRelationId, AccessShareLock);
    indscan = systable_beginscan(partrel, PartitionIndexTableIdIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(parttup = systable_getnext(indscan))) {
        Form_pg_partition partform = (Form_pg_partition)GETSTRUCT(parttup);
        Form_pg_index indexform;
        HeapTuple indexTup;
        Datum indclassDatum;
        oidvector* indclass = NULL;
        bool isnull = false;
        HeapTuple classTup;

        /* Search 'partform->parentid' in pg_class */
        classTup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(partform->parentid), LOG);

        if (!HeapTupleIsValid(classTup)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    (errmsg("cache lookup failed for relation %u", partform->parentid))));
        }
        /* Search indexrelid in pg_index */
        indexTup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(HeapTupleGetOid(classTup)));
        if (!HeapTupleIsValid(indexTup)) {
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    (errmsg("cache lookup failed for index %u", HeapTupleGetOid(classTup)))));
        }
        indexform = (Form_pg_index)GETSTRUCT(indexTup);

        /*
         * Ignore any indexes that are currently being dropped or unusable index
         */
        if (!IndexIsLive(indexform) && !inc_unused) {
            ReleaseSysCache(classTup);
            ReleaseSysCache(indexTup);
            continue;
        }

        /* Add index's OID to result list in the proper order */
        result = insert_ordered_oid(result, HeapTupleGetOid(parttup));
        /*
         * indclass cannot be referenced directly through the C struct,
         * because it comes after the variable-width indkey field.	Must
         * extract the datum the hard way...
         */
        indclassDatum = heap_getattr(indexTup, Anum_pg_index_indclass, GetLSCPgIndexDescriptor(), &isnull);

        Assert(!isnull);
        indclass = (oidvector*)DatumGetPointer(indclassDatum);
        if (!PointerIsValid(indclass)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Fail to get index key for index with oid %u", PartitionGetPartid(part))));
        }

        /* Check to see if it is a unique, non-partial btree index on OID */
        if (indexform->indnatts == 1 && indexform->indisunique && indexform->indimmediate &&
            indexform->indkey.values[0] == ObjectIdAttributeNumber && indclass->values[0] == OID_BTREE_OPS_OID &&
            heap_attisnull(indexTup, Anum_pg_index_indpred, NULL)) {
            oidIndex = indexform->indexrelid;
        }

        ReleaseSysCache(classTup);
        ReleaseSysCache(indexTup);
    }

    systable_endscan(indscan);
    heap_close(partrel, AccessShareLock);

    /* Now save a copy of the completed list in the relcache entry. */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());
    if (part->pd_indexlist) {
        list_free_ext(part->pd_indexlist);
    }
    part->pd_indexlist = list_copy(result);
    part->pd_oidindex = oidIndex;
    part->pd_indexvalid = 1;
    (void)MemoryContextSwitchTo(oldcxt);

    return result;
}

/*
 * Brief        : Determine whether the given relation is a "Dfs" store(HDFS table).
 * Input        : relation, a "RelationData*" struct.
 * Output       : None.
 * Return Value : Return true if the relation is Dfs store, otherwise retrun false .
 * Notes        : None.
 */
bool RelationIsDfsStore(Relation relation)
{
    Oid tablespaceId = InvalidOid;
    char* optionValue = NULL;
    bool isHdfsStore = false;
    Assert(NULL != relation);

    /* If the table is internal relation, the table would not be Dfs store. */
    if (RelationIsInternal(relation)) {
        return false;
    }

    tablespaceId = relation->rd_rel->reltablespace;
    if (OidIsValid(tablespaceId)) {
        optionValue = GetTablespaceOptionValue(tablespaceId, TABLESPACE_OPTION_FILESYSTEM);

        if (optionValue && 0 == pg_strncasecmp(optionValue, HDFS, strlen(HDFS))) {
            isHdfsStore = true;
        }
    }

    return isHdfsStore;
}

/*
 * Brief        : check whether the relation is hdfs table.
 * Input        : relation oid
 * Output       : None.
 * Return Value : ture if the relation is hdfs table, else false
 * Notes        : None.
 */
bool RelationIsPaxFormatByOid(Oid relid)
{
    bool rs = false;
    Relation rel;

    if (!OidIsValid(relid))
        return false;

    rel = try_relation_open(relid, AccessShareLock);
    if (NULL == rel)
        return false;

    rs = RelationIsPAXFormat(rel);
    relation_close(rel, AccessShareLock);

    return rs;
}

bool RelationIsCUFormatByOid(Oid relid)
{
    bool rs = false;
    Relation rel;

    if (!OidIsValid(relid))
        return false;

    rel = try_relation_open(relid, AccessShareLock);
    if (NULL == rel)
        return false;

    rs = RelationIsCUFormat(rel);
    relation_close(rel, AccessShareLock);

    return rs;
}

#ifdef ENABLE_MOT
/*
 * Brief        : check whether the relation is MOT table.
 * Input        : relation oid
 * Return Value : ture if the relation is MOT table, else false
 */
bool RelationIsMOTTableByOid(Oid relid)
{
    return isMOTFromTblOid(relid);
}
#endif

/*
 * Brief        : Check Relation redistribution status in pg_class
 * Input        : relation oid
 * Output       : None.
 * Return Value : ture if the relation is in redistribution
 * Notes        : None.
 */
bool CheckRelationInRedistribution(Oid rel_oid)
{
    Relation pgclass;
    HeapTuple tuple;
    bool isnull = false;
    Datum datum;
    bool ret = false;

    if (!OidIsValid(rel_oid))
        return false;

    pgclass = heap_open(RelationRelationId, RowShareLock);
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
    if (!HeapTupleIsValid(tuple)) {
        /*
         * If do not find 'rel_oid', we report error before like:
         * ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u",
         * rel_oid)));
         *
         * But at this time because we do not lock this oid, other process can change it by
         * 'drop+create the same table then commit'. In this case, we cannot find the oid
         * but should not report error, and go back to refresh syscahe and get the new oid.
         */
        heap_close(pgclass, RowShareLock);
        return false;
    }

    datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
    ret = CheckRelOptionValue(datum, "append_mode");

    ReleaseSysCache(tuple);
    heap_close(pgclass, RowShareLock);

    return ret;
}

/*
 * Brief        : package the meta data of the foreign table.
 * Input        : Relation
 * Return Value : RelationMetaData*, includs all necessary meta data during scan.
 * Notes        : None.
 */
RelationMetaData* make_relmeta(Relation rel)
{
    errno_t err = EOK;

    RelationMetaData* node = makeNode(RelationMetaData);

    node->rd_id = rel->rd_id;

    node->spcNode = rel->rd_node.spcNode;
    node->dbNode = rel->rd_node.dbNode;
    node->relNode = rel->rd_node.relNode;
    node->bucketNode = rel->rd_node.bucketNode;

    node->relname = (char*)palloc0(NAMEDATALEN);
    err = memcpy_s(node->relname, NAMEDATALEN, rel->rd_rel->relname.data, NAMEDATALEN);
    securec_check_c(err, "\0", "\0");

    node->relkind = rel->rd_rel->relkind;
    node->parttype = rel->rd_rel->parttype;

    node->natts = rel->rd_att->natts;
    for (int i = 0; i < rel->rd_att->natts; i++) {
        AttrMetaData* attr = makeNode(AttrMetaData);

        attr->attalign = rel->rd_att->attrs[i]->attalign;
        attr->attbyval = rel->rd_att->attrs[i]->attbyval;
        attr->attkvtype = rel->rd_att->attrs[i]->attkvtype;
        attr->attcmprmode = rel->rd_att->attrs[i]->attcmprmode;
        attr->attcollation = rel->rd_att->attrs[i]->attcollation;
        attr->atthasdef = rel->rd_att->attrs[i]->atthasdef;
        attr->attinhcount = rel->rd_att->attrs[i]->attinhcount;
        attr->attisdropped = rel->rd_att->attrs[i]->attisdropped;
        attr->attislocal = rel->rd_att->attrs[i]->attislocal;
        attr->attnotnull = rel->rd_att->attrs[i]->attnotnull;
        attr->attlen = rel->rd_att->attrs[i]->attlen;
        attr->attnum = rel->rd_att->attrs[i]->attnum;
        attr->attstorage = rel->rd_att->attrs[i]->attstorage;
        attr->atttypid = rel->rd_att->attrs[i]->atttypid;
        attr->atttypmod = rel->rd_att->attrs[i]->atttypmod;

        attr->attname = (char*)palloc0(NAMEDATALEN);
        err = memcpy_s(attr->attname, NAMEDATALEN, rel->rd_att->attrs[i]->attname.data, NAMEDATALEN);
        securec_check_c(err, "\0", "\0");

        node->attrs = lappend(node->attrs, attr);
    }

    return node;
}

/*
 * Brief        : unpackage the meta data of the foreign table.
 * Input        : RelationMetaData*, includs all necessary meta data during scan.
 * Return Value : Relation*
 * Notes        : None.
 */
Relation get_rel_from_meta(RelationMetaData* node)
{
    errno_t rc;

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE),
            "sizeof(RelationData): %u, sizeof(FormData_pg_class): %u, sizeof(tupleDesc): %u",
            sizeof(RelationData),
            sizeof(FormData_pg_class),
            sizeof(tupleDesc)));

    Relation rel = (Relation)palloc0(sizeof(RelationData));

    rel->rd_id = node->rd_id;

    rel->rd_node.spcNode = node->spcNode;
    rel->rd_node.dbNode = node->dbNode;
    rel->rd_node.relNode = node->relNode;
    rel->rd_node.bucketNode = node->bucketNode;

    rel->rd_rel = (Form_pg_class)palloc0(sizeof(FormData_pg_class));
    rel->rd_rel->relkind = node->relkind;
    rel->rd_rel->parttype = node->parttype;
    rc = memcpy_s(rel->rd_rel->relname.data, NAMEDATALEN, node->relname, strlen(node->relname));
    securec_check(rc, "", "");

    /* set tupdesc */
    rel->rd_att = CreateTemplateTupleDesc(node->natts, false);

    rel->rd_att->natts = node->natts;

    for (int i = 0; i < rel->rd_att->natts; i++) {
        AttrMetaData* attr = (AttrMetaData*)list_nth(node->attrs, i);

        rel->rd_att->attrs[i]->attalign = attr->attalign;
        rel->rd_att->attrs[i]->attbyval = attr->attbyval;
        rel->rd_att->attrs[i]->attkvtype = attr->attkvtype;
        rel->rd_att->attrs[i]->attcmprmode = attr->attcmprmode;
        rel->rd_att->attrs[i]->attcollation = attr->attcollation;
        rel->rd_att->attrs[i]->atthasdef = attr->atthasdef;
        rel->rd_att->attrs[i]->attinhcount = attr->attinhcount;
        rel->rd_att->attrs[i]->attisdropped = attr->attisdropped;
        rel->rd_att->attrs[i]->attislocal = attr->attislocal;
        rel->rd_att->attrs[i]->attnotnull = attr->attnotnull;
        rel->rd_att->attrs[i]->attlen = attr->attlen;
        rel->rd_att->attrs[i]->attnum = attr->attnum;
        rel->rd_att->attrs[i]->attstorage = attr->attstorage;
        rel->rd_att->attrs[i]->atttypid = attr->atttypid;
        rel->rd_att->attrs[i]->atttypmod = attr->atttypmod;

        rc = memcpy_s(rel->rd_att->attrs[i]->attname.data, NAMEDATALEN, attr->attname, strlen(attr->attname));
        securec_check(rc, "", "");
    }

    return rel;
}

/*
 * Brief        : Get the relation by the given pg_class tuple and it's schema.
 * Input        : pg_class_tuple, the tuple in pg_class; lockmode, lockmode; tuple_desc, cached schema; pg_index_tuple, the
 *                tuple in pg_index if it is an index.
 * Return Value : Relation*
 * Notes        : This function is used for timeseries, do not call this function directly.
 */
Relation tuple_get_rel(HeapTuple pg_class_tuple, LOCKMODE lockmode, TupleDesc tuple_desc, HeapTuple pg_index_tuple)
{
    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    MemoryContext oldcxt;

    Oid relid = HeapTupleGetOid(pg_class_tuple);
    if (lockmode != NoLock) {
        LockRelationOid(relid, lockmode);
    }
    if (EnableLocalSysCache()) {
        // local and global may has this relation
        Relation rel = t_thrd.lsc_cxt.lsc->tabdefcache.SearchRelation(relid);
        if (RelationIsValid(rel)) {
            pgstat_initstats(rel);
            RelationIncrementReferenceCount(rel);
            return rel;
        }
    }
    Form_pg_class relp = (Form_pg_class)GETSTRUCT(pg_class_tuple);
    /* allocate storage for the relation descriptor, and copy pg_class_tuple to relation->rd_rel. */
    Relation relation = AllocateRelationDesc(relp);
    /* initialize the relation's relation id (relation->rd_id) */
    RelationGetRelid(relation) = relid;
    relation->rd_refcnt = 0;
    relation->rd_isnailed = false;
    relation->rd_createSubid = InvalidSubTransactionId;
    relation->rd_backend = InvalidBackendId;
    relation->rd_islocaltemp = false;
    /* initialize the tuple descriptor (relation->rd_att). */
    oldcxt = MemoryContextSwitchTo(LocalMyDBCacheMemCxt());

    relation->rd_att = CreateTupleDescCopy(tuple_desc);
    relation->rd_att->tdtypeid = relation->rd_rel->reltype;
    relation->rd_att->tdtypmod = -1;
    relation->rd_att->tdhasoid = relation->rd_rel->relhasoids;
    (void)MemoryContextSwitchTo(oldcxt);
    /* set rules and triggers that affect this relation */
    relation->rd_rules = NULL;
    relation->rd_rulescxt = NULL;
    relation->trigdesc = NULL;
    /* 
     * If it's an index, initialize index-related information.
     * We modify RelationInitIndexAccessInfo interface to input index tuple which cached by ourself.
     */
    if (OidIsValid(relation->rd_rel->relam) && pg_index_tuple != NULL)
        RelationInitIndexAccessInfo(relation, pg_index_tuple);
    /* extract reloptions if any */
    RelationParseRelOptions(relation, pg_class_tuple);
    /* get row level security policies for this relation */
    relation->rd_rlsdesc = NULL;
    /* fetch bucket info from pgclass tuple */
    RelationInitBucketInfo(relation, pg_class_tuple);
    /* hash bucket columns cannot be changed, so there is no need to rebuild them */
    RelationInitBucketKey(relation, pg_class_tuple);
    relation->parentId = InvalidOid;
    /* initialize the relation lock manager information */
    RelationInitLockInfo(relation); /* see lmgr.c */
    /* initialize physical addressing information for the relation */
    RelationInitPhysicalAddr(relation); 
    /* make sure relation is marked as having no open file yet */
    relation->rd_smgr = NULL; 
    /* It's fully valid */
    relation->rd_isvalid = true;

    pgstat_initstats(relation);
    relation->rd_att->tdrefcount = 1;
    RelationIdCacheInsertIntoLocal(relation);
    if (RelationIsValid(relation)) {
        RelationIncrementReferenceCount(relation);
    }
    return relation;
}

void GetTdeInfoFromRel(Relation rel, TdeInfo *tde_info)
{
    Assert(tde_info != NULL);
    errno_t rc = 0;
    rc = memset_s(tde_info, sizeof(TdeInfo), 0, sizeof(TdeInfo));
    securec_check(rc, "\0", "\0");

    const char* dek_cipher = RelationGetDekCipher(rel);
    const char* cmk_id = RelationGetCmkId(rel);
    const char* algo = RelationGetAlgo(rel);
    Assert(dek_cipher != NULL);
    Assert(cmk_id != NULL);
    Assert(algo != NULL);

    if (dek_cipher == NULL || cmk_id == NULL || algo == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("Failed to get tde info from relation \"%s\".", RelationGetRelationName(rel)),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact engineer to support.")));
        return;
    }

    rc = strcpy_s(tde_info->dek_cipher, DEK_CIPHER_LEN, dek_cipher);
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(tde_info->cmk_id, CMK_ID_LEN, cmk_id);
    securec_check(rc, "\0", "\0");
    if (pg_strcasecmp((char*)algo, "AES_128_CTR") == 0) {
        tde_info->algo = (uint8)TDE_ALGO_AES_128_CTR;
    } else if (pg_strcasecmp((char*)algo, "SM4_CTR") == 0) {
        tde_info->algo = (uint8)TDE_ALGO_SM4_CTR;
    } else {
        tde_info->algo = (uint8)TDE_ALGO_NONE;
    }
}

void SetupPageCompressForRelation(RelFileNode* node, PageCompressOpts* compress_options, const char* relationName)
{
    uint1 algorithm = compress_options->compressType;
    if (algorithm == COMPRESS_TYPE_NONE) {
        node->opt = 0;
    } else {
        if (!SUPPORT_PAGE_COMPRESSION) {
            ereport(ERROR, (errmsg("unsupported page compression on this platform")));
        }

        uint1 compressLevel;
        bool symbol = false;
        if (compress_options->compressLevel >= 0) {
            symbol = true;
            compressLevel = compress_options->compressLevel;
        } else {
            symbol = false;
            compressLevel = -compress_options->compressLevel;
        }
        bool success = false;
        uint1 chunkSize = ConvertChunkSize(compress_options->compressChunkSize, &success);
        if (!success) {
            ereport(ERROR, (errmsg("invalid compress_chunk_size %d , must be one of %d, %d, %d or %d for %s",
                                   compress_options->compressChunkSize, BLCKSZ / 16, BLCKSZ / 8, BLCKSZ / 4, BLCKSZ / 2,
                                   relationName)));
        }
        uint1 preallocChunks = 0;
        if (compress_options->compressPreallocChunks >= BLCKSZ / compress_options->compressChunkSize) {
            ereport(ERROR, (errmsg("invalid compress_prealloc_chunks %d , must be less than %d for %s",
                                   compress_options->compressPreallocChunks,
                                   BLCKSZ / compress_options->compressChunkSize, relationName)));
        } else {
            preallocChunks = (uint1)(compress_options->compressPreallocChunks);
        }
        node->opt = 0;
        SET_COMPRESS_OPTION((*node), compress_options->compressByteConvert, compress_options->compressDiffConvert,
                            preallocChunks, symbol, compressLevel, algorithm, chunkSize);
    }
}
char RelationGetRelReplident(Relation r)
{
    bool isNull = false;
    char relreplident;

    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(r)));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("cache lookup failed for relation %u", RelationGetRelid(r))));
    }

    Relation classRel = heap_open(RelationRelationId, AccessShareLock);
    Datum datum = heap_getattr(tuple, Anum_pg_class_relreplident, RelationGetDescr(classRel), &isNull);
    if (isNull) {
        relreplident = REPLICA_IDENTITY_NOTHING;
    } else {
        relreplident = CharGetDatum(datum);
    }
    
    heap_close(classRel, AccessShareLock);
    ReleaseSysCache(tuple);

    return relreplident;
}

