/* -------------------------------------------------------------------------
 *
 * indexing.h
 *	  This file provides some definitions to support indexing
 *	  on system catalogs
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/indexing.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef INDEXING_H
#define INDEXING_H

#include "access/htup.h"
#include "utils/relcache.h"

/*
 * The state object used by CatalogOpenIndexes and friends is actually the
 * same as the executor's ResultRelInfo, but we give it another type name
 * to decouple callers from that fact.
 */
typedef struct ResultRelInfo *CatalogIndexState;

/*
 * indexing.c prototypes
 */
extern CatalogIndexState CatalogOpenIndexes(Relation heapRel);
extern void CatalogCloseIndexes(CatalogIndexState indstate);
extern void CatalogIndexInsert(CatalogIndexState indstate,
				   HeapTuple heapTuple);
extern void CatalogUpdateIndexes(Relation heapRel, HeapTuple heapTuple);


/*
 * These macros are just to keep the C compiler from spitting up on the
 * upcoming commands for genbki.pl.
 */
#define DECLARE_INDEX(name,oid,decl) extern int no_such_variable
#define DECLARE_UNIQUE_INDEX(name,oid,decl) extern int no_such_variable
#define BUILD_INDICES


/*
 * What follows are lines processed by genbki.pl to create the statements
 * the bootstrap parser will turn into DefineIndex commands.
 *
 * The keyword is DECLARE_INDEX or DECLARE_UNIQUE_INDEX.  The first two
 * arguments are the index name and OID, the rest is much like a standard
 * 'create index' SQL command.
 *
 * For each index, we also provide a #define for its OID.  References to
 * the index in the C code should always use these #defines, not the actual
 * index name (much less the numeric OID).
 */

DECLARE_UNIQUE_INDEX(pg_aggregate_fnoid_index, 2650, on pg_aggregate using btree(aggfnoid oid_ops));
#define AggregateFnoidIndexId  2650

DECLARE_UNIQUE_INDEX(pg_am_name_index, 2651, on pg_am using btree(amname name_ops));
#define AmNameIndexId  2651
DECLARE_UNIQUE_INDEX(pg_am_oid_index, 2652, on pg_am using btree(oid oid_ops));
#define AmOidIndexId  2652

DECLARE_UNIQUE_INDEX(pg_amop_fam_strat_index, 2653, on pg_amop using btree(amopfamily oid_ops, amoplefttype oid_ops, amoprighttype oid_ops, amopstrategy int2_ops));
#define AccessMethodStrategyIndexId  2653
DECLARE_UNIQUE_INDEX(pg_amop_opr_fam_index, 2654, on pg_amop using btree(amopopr oid_ops, amoppurpose char_ops, amopfamily oid_ops));
#define AccessMethodOperatorIndexId  2654
DECLARE_UNIQUE_INDEX(pg_amop_oid_index, 2756, on pg_amop using btree(oid oid_ops));
#define AccessMethodOperatorOidIndexId	2756

DECLARE_UNIQUE_INDEX(pg_amproc_fam_proc_index, 2655, on pg_amproc using btree(amprocfamily oid_ops, amproclefttype oid_ops, amprocrighttype oid_ops, amprocnum int2_ops));
#define AccessMethodProcedureIndexId  2655
DECLARE_UNIQUE_INDEX(pg_amproc_oid_index, 2757, on pg_amproc using btree(oid oid_ops));
#define AccessMethodProcedureOidIndexId  2757

DECLARE_UNIQUE_INDEX(pg_attrdef_adrelid_adnum_index, 2656, on pg_attrdef using btree(adrelid oid_ops, adnum int2_ops));
#define AttrDefaultIndexId	2656
DECLARE_UNIQUE_INDEX(pg_attrdef_oid_index, 2657, on pg_attrdef using btree(oid oid_ops));
#define AttrDefaultOidIndexId  2657

DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnam_index, 2658, on pg_attribute using btree(attrelid oid_ops, attname name_ops));
#define AttributeRelidNameIndexId  2658
DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnum_index, 2659, on pg_attribute using btree(attrelid oid_ops, attnum int2_ops));
#define AttributeRelidNumIndexId  2659

DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, on pg_authid using btree(rolname name_ops));
#define AuthIdRolnameIndexId	2676
DECLARE_UNIQUE_INDEX(pg_authid_oid_index, 2677, on pg_authid using btree(oid oid_ops));
#define AuthIdOidIndexId	2677

DECLARE_UNIQUE_INDEX(pg_auth_members_role_member_index, 2694, on pg_auth_members using btree(roleid oid_ops, member oid_ops));
#define AuthMemRoleMemIndexId	2694
DECLARE_UNIQUE_INDEX(pg_auth_members_member_role_index, 2695, on pg_auth_members using btree(member oid_ops, roleid oid_ops));
#define AuthMemMemRoleIndexId	2695

/* Add index for gs_db_privilege */
DECLARE_UNIQUE_INDEX(gs_db_privilege_oid_index, 5568, on gs_db_privilege using btree(oid oid_ops));
#define DbPrivilegeOidIndexId 5568
DECLARE_INDEX(gs_db_privilege_roleid_index, 5569, on gs_db_privilege using btree(roleid oid_ops));
#define DbPrivilegeRoleidIndexId 5569
DECLARE_UNIQUE_INDEX(gs_db_privilege_roleid_privilege_type_index, 5570, on gs_db_privilege using btree(roleid oid_ops, privilege_type text_ops));
#define DbPrivilegeRoleidPrivilegeTypeIndexId 5570

DECLARE_UNIQUE_INDEX(pg_cast_oid_index, 2660, on pg_cast using btree(oid oid_ops));
#define CastOidIndexId	2660
DECLARE_UNIQUE_INDEX(pg_cast_source_target_index, 2661, on pg_cast using btree(castsource oid_ops, casttarget oid_ops));
#define CastSourceTargetIndexId  2661

DECLARE_UNIQUE_INDEX(pg_class_oid_index, 2662, on pg_class using btree(oid oid_ops));
#define ClassOidIndexId  2662
DECLARE_UNIQUE_INDEX(pg_class_relname_nsp_index, 2663, on pg_class using btree(relname name_ops, relnamespace oid_ops));
#define ClassNameNspIndexId  2663
DECLARE_INDEX(pg_class_tblspc_relfilenode_index, 9981, on pg_class using btree(reltablespace oid_ops, relfilenode oid_ops));
#define ClassTblspcRelfilenodeIndexId  9981
DECLARE_UNIQUE_INDEX(pg_collation_name_enc_nsp_index, 3164, on pg_collation using btree(collname name_ops, collencoding int4_ops, collnamespace oid_ops));
#define CollationNameEncNspIndexId 3164
DECLARE_UNIQUE_INDEX(pg_collation_oid_index, 3085, on pg_collation using btree(oid oid_ops));
#define CollationOidIndexId  3085

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_conname_nsp_index, 2664, on pg_constraint using btree(conname name_ops, connamespace oid_ops));
#define ConstraintNameNspIndexId  2664
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_conrelid_index, 2665, on pg_constraint using btree(conrelid oid_ops));
#define ConstraintRelidIndexId	2665
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_constraint_contypid_index, 2666, on pg_constraint using btree(contypid oid_ops));
#define ConstraintTypidIndexId	2666
DECLARE_UNIQUE_INDEX(pg_constraint_oid_index, 2667, on pg_constraint using btree(oid oid_ops));
#define ConstraintOidIndexId  2667

DECLARE_UNIQUE_INDEX(pg_conversion_default_index, 2668, on pg_conversion using btree(connamespace oid_ops, conforencoding int4_ops, contoencoding int4_ops, oid oid_ops));
#define ConversionDefaultIndexId  2668
DECLARE_UNIQUE_INDEX(pg_conversion_name_nsp_index, 2669, on pg_conversion using btree(conname name_ops, connamespace oid_ops));
#define ConversionNameNspIndexId  2669
DECLARE_UNIQUE_INDEX(pg_conversion_oid_index, 2670, on pg_conversion using btree(oid oid_ops));
#define ConversionOidIndexId  2670

DECLARE_UNIQUE_INDEX(pg_database_datname_index, 2671, on pg_database using btree(datname name_ops));
#define DatabaseNameIndexId  2671
DECLARE_UNIQUE_INDEX(pg_database_oid_index, 2672, on pg_database using btree(oid oid_ops));
#define DatabaseOidIndexId	2672

/* This follwing index is used for Data Source */
DECLARE_UNIQUE_INDEX(pg_extension_data_source_oid_index, 7166, on pg_extension_data_source using btree(oid oid_ops));
#define DataSourceOidIndexId	7166

DECLARE_UNIQUE_INDEX(pg_extension_data_source_name_index, 7167, on pg_extension_data_source using btree(srcname name_ops));
#define DataSourceNameIndexId	7167

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_depend_depender_index, 2673, on pg_depend using btree(classid oid_ops, objid oid_ops, objsubid int4_ops));
#define DependDependerIndexId  2673
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_depend_reference_index, 2674, on pg_depend using btree(refclassid oid_ops, refobjid oid_ops, refobjsubid int4_ops));
#define DependReferenceIndexId	2674

DECLARE_UNIQUE_INDEX(pg_description_o_c_o_index, 2675, on pg_description using btree(objoid oid_ops, classoid oid_ops, objsubid int4_ops));
#define DescriptionObjIndexId  2675
DECLARE_UNIQUE_INDEX(pg_shdescription_o_c_index, 2397, on pg_shdescription using btree(objoid oid_ops, classoid oid_ops));
#define SharedDescriptionObjIndexId 2397

DECLARE_UNIQUE_INDEX(pg_enum_oid_index, 3502, on pg_enum using btree(oid oid_ops));
#define EnumOidIndexId	3502
DECLARE_UNIQUE_INDEX(pg_enum_typid_label_index, 3503, on pg_enum using btree(enumtypid oid_ops, enumlabel name_ops));
#define EnumTypIdLabelIndexId 3503
DECLARE_UNIQUE_INDEX(pg_enum_typid_sortorder_index, 3534, on pg_enum using btree(enumtypid oid_ops, enumsortorder float4_ops));
#define EnumTypIdSortOrderIndexId 3534

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_index_indrelid_index, 2678, on pg_index using btree(indrelid oid_ops));
#define IndexIndrelidIndexId  2678
DECLARE_UNIQUE_INDEX(pg_index_indexrelid_index, 2679, on pg_index using btree(indexrelid oid_ops));
#define IndexRelidIndexId  2679

DECLARE_UNIQUE_INDEX(pg_inherits_relid_seqno_index, 2680, on pg_inherits using btree(inhrelid oid_ops, inhseqno int4_ops));
#define InheritsRelidSeqnoIndexId  2680
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_inherits_parent_index, 2187, on pg_inherits using btree(inhparent oid_ops));
#define InheritsParentIndexId  2187

DECLARE_UNIQUE_INDEX(pg_language_name_index, 2681, on pg_language using btree(lanname name_ops));
#define LanguageNameIndexId  2681
DECLARE_UNIQUE_INDEX(pg_language_oid_index, 2682, on pg_language using btree(oid oid_ops));
#define LanguageOidIndexId	2682

DECLARE_UNIQUE_INDEX(pg_largeobject_loid_pn_index, 2683, on pg_largeobject using btree(loid oid_ops, pageno int4_ops));
#define LargeObjectLOidPNIndexId  2683

DECLARE_UNIQUE_INDEX(pg_largeobject_metadata_oid_index, 2996, on pg_largeobject_metadata using btree(oid oid_ops));
#define LargeObjectMetadataOidIndexId	2996

DECLARE_UNIQUE_INDEX(pg_namespace_nspname_index, 2684, on pg_namespace using btree(nspname name_ops));
#define NamespaceNameIndexId  2684
DECLARE_UNIQUE_INDEX(pg_namespace_oid_index, 2685, on pg_namespace using btree(oid oid_ops));
#define NamespaceOidIndexId  2685

DECLARE_UNIQUE_INDEX(pg_opclass_am_name_nsp_index, 2686, on pg_opclass using btree(opcmethod oid_ops, opcname name_ops, opcnamespace oid_ops));
#define OpclassAmNameNspIndexId  2686
DECLARE_UNIQUE_INDEX(pg_opclass_oid_index, 2687, on pg_opclass using btree(oid oid_ops));
#define OpclassOidIndexId  2687

DECLARE_UNIQUE_INDEX(pg_operator_oid_index, 2688, on pg_operator using btree(oid oid_ops));
#define OperatorOidIndexId	2688
DECLARE_UNIQUE_INDEX(pg_operator_oprname_l_r_n_index, 2689, on pg_operator using btree(oprname name_ops, oprleft oid_ops, oprright oid_ops, oprnamespace oid_ops));
#define OperatorNameNspIndexId	2689

DECLARE_UNIQUE_INDEX(pg_opfamily_am_name_nsp_index, 2754, on pg_opfamily using btree(opfmethod oid_ops, opfname name_ops, opfnamespace oid_ops));
#define OpfamilyAmNameNspIndexId  2754
DECLARE_UNIQUE_INDEX(pg_opfamily_oid_index, 2755, on pg_opfamily using btree(oid oid_ops));
#define OpfamilyOidIndexId	2755

DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137

DECLARE_UNIQUE_INDEX(pg_proc_oid_index, 2690, on pg_proc using btree(oid oid_ops));
#define ProcedureOidIndexId  2690

DECLARE_INDEX(pg_proc_proname_args_nsp_index, 2691, on pg_proc using btree(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops));
#define ProcedureNameArgsNspIndexId  2691
DECLARE_INDEX(pg_proc_proname_all_args_nsp_index, 9666, on pg_proc using btree(proname name_ops, allargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops));
#define ProcedureNameAllArgsNspIndexId  9666

DECLARE_INDEX(pg_proc_proname_args_nsp_new_index, 9378, on pg_proc using btree(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops, propackageid oid_ops));
#define ProcedureNameArgsNspNewIndexId  9378

DECLARE_UNIQUE_INDEX(pg_rewrite_oid_index, 2692, on pg_rewrite using btree(oid oid_ops));
#define RewriteOidIndexId  2692
DECLARE_UNIQUE_INDEX(pg_rewrite_rel_rulename_index, 2693, on pg_rewrite using btree(ev_class oid_ops, rulename name_ops));
#define RewriteRelRulenameIndexId  2693

DECLARE_UNIQUE_INDEX(gs_package_oid_index, 9993, on gs_package using btree(oid oid_ops));
#define PackageOidIndexId  9993

DECLARE_UNIQUE_INDEX(gs_package_name_index, 9736, on gs_package using btree(pkgname name_ops, pkgnamespace oid_ops));
#define PackageNameIndexId  9736

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_shdepend_depender_index, 1232, on pg_shdepend using btree(dbid oid_ops, classid oid_ops, objid oid_ops, objsubid int4_ops));
#define SharedDependDependerIndexId		1232
/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_shdepend_reference_index, 1233, on pg_shdepend using btree(refclassid oid_ops, refobjid oid_ops));
#define SharedDependReferenceIndexId	1233

DECLARE_UNIQUE_INDEX(pg_statistic_relid_kind_att_inh_index, 2696, on pg_statistic using btree(starelid oid_ops, starelkind char_ops, staattnum int2_ops, stainherit bool_ops));
#define StatisticRelidKindAttnumInhIndexId 2696

DECLARE_UNIQUE_INDEX(pg_statistic_ext_relid_kind_inh_key_index, 3222, on pg_statistic_ext using btree(starelid oid_ops, starelkind char_ops, stainherit bool_ops, stakey array_ops));
#define StatisticExtRelidKindInhKeyIndexId 3222

DECLARE_UNIQUE_INDEX(pg_tablespace_oid_index, 2697, on pg_tablespace using btree(oid oid_ops));
#define TablespaceOidIndexId  2697
DECLARE_UNIQUE_INDEX(pg_tablespace_spcname_index, 2698, on pg_tablespace using btree(spcname name_ops));
#define TablespaceNameIndexId  2698

/* This following index is not used for a cache and is not unique */
DECLARE_INDEX(pg_trigger_tgconstraint_index, 2699, on pg_trigger using btree(tgconstraint oid_ops));
#define TriggerConstraintIndexId  2699
DECLARE_UNIQUE_INDEX(pg_trigger_tgrelid_tgname_index, 2701, on pg_trigger using btree(tgrelid oid_ops, tgname name_ops));
#define TriggerRelidNameIndexId  2701
DECLARE_UNIQUE_INDEX(pg_trigger_oid_index, 2702, on pg_trigger using btree(oid oid_ops));
#define TriggerOidIndexId  2702

DECLARE_UNIQUE_INDEX(pg_ts_config_cfgname_index, 3608, on pg_ts_config using btree(cfgname name_ops, cfgnamespace oid_ops));
#define TSConfigNameNspIndexId	3608
DECLARE_UNIQUE_INDEX(pg_ts_config_oid_index, 3712, on pg_ts_config using btree(oid oid_ops));
#define TSConfigOidIndexId	3712

DECLARE_UNIQUE_INDEX(pg_ts_config_map_index, 3609, on pg_ts_config_map using btree(mapcfg oid_ops, maptokentype int4_ops, mapseqno int4_ops));
#define TSConfigMapIndexId	3609

DECLARE_UNIQUE_INDEX(pg_ts_dict_dictname_index, 3604, on pg_ts_dict using btree(dictname name_ops, dictnamespace oid_ops));
#define TSDictionaryNameNspIndexId	3604
DECLARE_UNIQUE_INDEX(pg_ts_dict_oid_index, 3605, on pg_ts_dict using btree(oid oid_ops));
#define TSDictionaryOidIndexId	3605

DECLARE_UNIQUE_INDEX(pg_ts_parser_prsname_index, 3606, on pg_ts_parser using btree(prsname name_ops, prsnamespace oid_ops));
#define TSParserNameNspIndexId	3606
DECLARE_UNIQUE_INDEX(pg_ts_parser_oid_index, 3607, on pg_ts_parser using btree(oid oid_ops));
#define TSParserOidIndexId	3607

DECLARE_UNIQUE_INDEX(pg_ts_template_tmplname_index, 3766, on pg_ts_template using btree(tmplname name_ops, tmplnamespace oid_ops));
#define TSTemplateNameNspIndexId	3766
DECLARE_UNIQUE_INDEX(pg_ts_template_oid_index, 3767, on pg_ts_template using btree(oid oid_ops));
#define TSTemplateOidIndexId	3767

DECLARE_UNIQUE_INDEX(pg_type_oid_index, 2703, on pg_type using btree(oid oid_ops));
#define TypeOidIndexId	2703
DECLARE_UNIQUE_INDEX(pg_type_typname_nsp_index, 2704, on pg_type using btree(typname name_ops, typnamespace oid_ops));
#define TypeNameNspIndexId	2704

DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_oid_index, 112, on pg_foreign_data_wrapper using btree(oid oid_ops));
#define ForeignDataWrapperOidIndexId	112

DECLARE_UNIQUE_INDEX(pg_foreign_data_wrapper_name_index, 548, on pg_foreign_data_wrapper using btree(fdwname name_ops));
#define ForeignDataWrapperNameIndexId	548

DECLARE_UNIQUE_INDEX(pg_foreign_server_oid_index, 113, on pg_foreign_server using btree(oid oid_ops));
#define ForeignServerOidIndexId 113

DECLARE_UNIQUE_INDEX(pg_foreign_server_name_index, 549, on pg_foreign_server using btree(srvname name_ops));
#define ForeignServerNameIndexId	549

DECLARE_UNIQUE_INDEX(pg_user_mapping_oid_index, 174, on pg_user_mapping using btree(oid oid_ops));
#define UserMappingOidIndexId	174

DECLARE_UNIQUE_INDEX(pg_user_mapping_user_server_index, 175, on pg_user_mapping using btree(umuser oid_ops, umserver oid_ops));
#define UserMappingUserServerIndexId	175

#ifdef PGXC
DECLARE_UNIQUE_INDEX(pgxc_class_pcrelid_index, 9002, on pgxc_class using btree(pcrelid oid_ops));
#define PgxcClassPgxcRelIdIndexId 	9002

DECLARE_UNIQUE_INDEX(pgxc_node_oid_index, 9010, on pgxc_node using btree(oid oid_ops));
#define PgxcNodeOidIndexId			9010

DECLARE_UNIQUE_INDEX(pgxc_node_name_type_index, 9024, on pgxc_node using btree(node_name name_ops, node_type char_ops, oid oid_ops));
#define PgxcNodeNodeNameIndexId 	9024
#define PgxcNodeNodeNameIndexIdOld 	9011

DECLARE_UNIQUE_INDEX(pgxc_group_name_index, 9012, on pgxc_group using btree(group_name name_ops));
#define PgxcGroupGroupNameIndexId 	9012

DECLARE_UNIQUE_INDEX(pgxc_group_oid, 9013, on pgxc_group using btree(oid oid_ops));
#define PgxcGroupOidIndexId			9013

DECLARE_UNIQUE_INDEX(pg_resource_pool_name_index, 9000, on pg_resource_pool using btree(respool_name name_ops));
#define ResourcePoolPoolNameIndexId 	9000

DECLARE_UNIQUE_INDEX(pg_resource_pool_oid_index, 9017, on pg_resource_pool using btree(oid oid_ops));
#define ResourcePoolOidIndexId			9017

DECLARE_UNIQUE_INDEX(pg_workload_group_name_index, 9018, on pg_workload_group using btree(workload_gpname name_ops));
#define WorkloadGroupGroupNameIndexId 	9018

DECLARE_UNIQUE_INDEX(pg_workload_group_oid_index, 9019, on pg_workload_group using btree(oid oid_ops));
#define WorkloadGroupOidIndexId			9019

DECLARE_UNIQUE_INDEX(pg_app_workloadgroup_mapping_name_index, 9020, on pg_app_workloadgroup_mapping using btree(appname name_ops));
#define AppWorkloadGroupMappingNameIndexId	9020

DECLARE_UNIQUE_INDEX(pg_app_workloadgroup_mapping_oid_index, 9021, on pg_app_workloadgroup_mapping using btree(oid oid_ops));
#define AppWorkloadGroupMappingOidIndexId	9021

DECLARE_UNIQUE_INDEX(pgxc_node_id_index, 9003, on pgxc_node using btree(node_id int4_ops));
#define PgxcNodeNodeIdIndexId 	9003

DECLARE_UNIQUE_INDEX(pgxc_slice_relid_index, 9033, on pgxc_slice using btree(relid oid_ops, type char_ops, relname name_ops, sindex int4_ops));
#define PgxcSliceIndexId 9033

DECLARE_UNIQUE_INDEX(pgxc_slice_order_index, 9034, on pgxc_slice using btree(relid oid_ops, type char_ops, sliceorder int4_ops, sindex int4_ops));
#define PgxcSliceOrderIndexId 9034

#endif

DECLARE_UNIQUE_INDEX(pg_foreign_table_relid_index, 3119, on pg_foreign_table using btree(ftrelid oid_ops));
#define ForeignTableRelidIndexId 3119

DECLARE_UNIQUE_INDEX(pg_default_acl_role_nsp_obj_index, 827, on pg_default_acl using btree(defaclrole oid_ops, defaclnamespace oid_ops, defaclobjtype char_ops));
#define DefaultAclRoleNspObjIndexId 827
DECLARE_UNIQUE_INDEX(pg_default_acl_oid_index, 828, on pg_default_acl using btree(oid oid_ops));
#define DefaultAclOidIndexId	828

DECLARE_UNIQUE_INDEX(pg_db_role_setting_databaseid_rol_index, 2965, on pg_db_role_setting using btree(setdatabase oid_ops, setrole oid_ops));
#define DbRoleSettingDatidRolidIndexId	2965

DECLARE_UNIQUE_INDEX(pg_seclabel_object_index, 3597, on pg_seclabel using btree(objoid oid_ops, classoid oid_ops, objsubid int4_ops, provider text_ops));
#define SecLabelObjectIndexId				3597

DECLARE_UNIQUE_INDEX(pg_shseclabel_object_index, 3593, on pg_shseclabel using btree(objoid oid_ops, classoid oid_ops, provider text_ops));
#define SharedSecLabelObjectIndexId			3593

DECLARE_UNIQUE_INDEX(pg_extension_oid_index, 3080, on pg_extension using btree(oid oid_ops));
#define ExtensionOidIndexId 3080

DECLARE_UNIQUE_INDEX(pg_extension_name_index, 3081, on pg_extension using btree(extname name_ops));
#define ExtensionNameIndexId 3081

DECLARE_UNIQUE_INDEX(pg_range_rngtypid_index, 3542, on pg_range using btree(rngtypid oid_ops));
#define RangeTypidIndexId					3542

/* add index of passwordtime for pg_auth_history */
DECLARE_UNIQUE_INDEX(pg_auth_history_index, 3458, on pg_auth_history using btree(roloid oid_ops, passwordtime timestamptz_ops));
#define AuthHistoryIndexId  3458
DECLARE_UNIQUE_INDEX(pg_auth_history_oid_index, 3459, on pg_auth_history using btree(oid oid_ops));
#define AuthHistoryOidIndexId	3459
/* add index of roleid for pg_user_status */
DECLARE_UNIQUE_INDEX(pg_user_status_index, 3461, on pg_user_status using btree(roloid oid_ops));
#define UserStatusRoleidIndexId  3461
DECLARE_UNIQUE_INDEX(pg_user_status_oid_index, 3462, on pg_user_status using btree(oid oid_ops));
#define UserStatusOidIndexId	3462
DECLARE_UNIQUE_INDEX(pg_partition_reloid_index, 3472, on pg_partition using btree(oid oid_ops));
#define PartitionOidIndexId  3472
DECLARE_INDEX(pg_partition_parentoid_index,3473, on pg_partition using btree(parttype char_ops, parentid oid_ops));
#define PartitionParentOidIndexId 3473
DECLARE_INDEX(pg_partition_indextblid_index,3474, on pg_partition using btree(indextblid oid_ops));
#define PartitionIndexTableIdIndexId 3474
DECLARE_UNIQUE_INDEX(pg_partition_partoid_index, 3479, on pg_partition using btree(relname name_ops, parttype char_ops, parentid oid_ops));
#define PartitionPartOidIndexId  3479
/* Add index of indextable and parent oid for pg_partition */
DECLARE_UNIQUE_INDEX(pg_partition_indextblid_parentoid_reloid_index, 9996, on pg_partition using btree(indextblid oid_ops, parentid oid_ops, oid oid_ops));
#define PartitionIndexTableIdParentOidIndexId  9996


/* Add index of table oid for pg_hashbucket */
DECLARE_UNIQUE_INDEX(pg_hashbucket_oid_index, 3492, on pg_hashbucket using btree(oid oid_ops));
#define HashBucketOidIndexId  3492
DECLARE_INDEX(pg_hashbucket_bid_index,3493, on pg_hashbucket using btree(bucketid oid_ops, bucketcnt int4_ops, bucketmapsize int4_ops));
#define HashBucketBidIndexId 3493

/* Add index of table oid for gs_uid */
DECLARE_UNIQUE_INDEX(gs_uid_relid_index, 3499, on gs_uid using btree(relid oid_ops));
#define UidRelidIndexId  3499

/* Add index of table oid for pg_job, pg_job_proc */
DECLARE_UNIQUE_INDEX(pg_job_oid_index, 3453, on pg_job using btree(oid oid_ops));
#define PgJobOidIndexId	3453
DECLARE_UNIQUE_INDEX(pg_job_id_index, 3454, on pg_job using btree(job_id int8_ops));
#define PgJobIdIndexId  3454
DECLARE_UNIQUE_INDEX(pg_job_proc_oid_index, 3455, on pg_job_proc using btree(oid oid_ops));
#define PgJobProcOidIndexId	3455
DECLARE_UNIQUE_INDEX(pg_job_proc_id_index, 3449, on pg_job_proc using btree(job_id int4_ops));
#define PgJobProcIdIndexId	3449

/* Add index of gs_job_argument */
DECLARE_UNIQUE_INDEX(gs_job_argument_oid_index, 4458, on gs_job_argument using btree(oid oid_ops));
#define GsJobArgumentProcOidIndexId 4458
DECLARE_UNIQUE_INDEX(gs_job_argument_name_index, 4459, on gs_job_argument using btree(job_name text_ops, argument_name text_ops));
#define GsJobArgumentNameIndexId	4459
DECLARE_UNIQUE_INDEX(gs_job_argument_position_index, 4460, on gs_job_argument using btree(job_name text_ops, argument_position int4_ops));
#define GsJobArgumentPositionIndexId	4460

/* Add index of gs_job_attribute */
DECLARE_UNIQUE_INDEX(gs_job_attribute_oid_index, 4456, on gs_job_attribute using btree(oid oid_ops));
#define GsJobAttributeProcOidIndexId 4456
DECLARE_UNIQUE_INDEX(gs_job_attribute_name_index, 4457, on gs_job_attribute using btree(job_name text_ops, attribute_name text_ops));
#define GsJobAttributeNameIndexId	4457

DECLARE_INDEX(gs_asp_sampletime_index, 2999, on gs_asp using btree(sample_time timestamptz_ops));
#define GsAspSampleIdTimedexId 2999

/* Add index of table oid for pg_directory */
DECLARE_UNIQUE_INDEX(pg_directory_oid_index, 4349, on pg_directory using btree(oid oid_ops));
#define PgDirectoryOidIndexId 4349
DECLARE_UNIQUE_INDEX(pg_directory_name_index, 4350, on pg_directory using btree(dirname name_ops));
#define PgDirectoryDirectoriesNameIndexId 4350

/* Add index of table oid for gs_global_chain */
DECLARE_INDEX(gs_global_chain_relid_index, 5511, on gs_global_chain using btree(relid oid_ops));
#define GsGlobalChainRelidIndexId 5511

/* Add index of table oid for gs_policy_label */
DECLARE_UNIQUE_INDEX(gs_policy_label_oid_index, 9501, on gs_policy_label using btree(oid oid_ops));
#define GsPolicyLabelOidIndexId  9501
DECLARE_INDEX(gs_policy_label_name_index, 9502, on gs_policy_label using btree(labelname name_ops, fqdnnamespace oid_ops, fqdnid oid_ops));
#define GsPolicyLabelNameIndexId  9502

/* Add index of table oid for gs_auditing_policy */
DECLARE_UNIQUE_INDEX(gs_auditing_policy_oid_index, 9511, on gs_auditing_policy using btree(oid oid_ops));
#define GsAuditingPolicyOidIndexId  9511
DECLARE_UNIQUE_INDEX(gs_auditing_policy_name_index, 9512, on gs_auditing_policy using btree(polname name_ops));
#define GsAuditingPolicyNameIndexId 9512

/* Add index of table oid for policy access and privileges */
DECLARE_UNIQUE_INDEX(gs_auditing_policy_access_oid_index, 9521, on gs_auditing_policy_access using btree(oid oid_ops));
#define GsAuditingPolicyAccessOidIndexId 9521
DECLARE_UNIQUE_INDEX(gs_auditing_policy_privileges_oid_index, 9531, on gs_auditing_policy_privileges using btree(oid oid_ops));
#define GsAuditingPolicyPrivilegesOidIndexId 9531

/* Add index for whole policy access and privileges row */
DECLARE_UNIQUE_INDEX(gs_auditing_policy_access_row_index, 9522, on gs_auditing_policy_access using btree(accesstype name_ops, labelname name_ops, policyoid oid_ops));
#define GsAuditingPolicyAccessRowIndexId 9522
DECLARE_UNIQUE_INDEX(gs_auditing_policy_privileges_row_index, 9532, on gs_auditing_policy_privileges using btree(privilegetype name_ops, labelname name_ops, policyoid oid_ops));
#define GsAuditingPolicyPrivilegesRowIndexId 9532

/* Add index for table oid for auditing policy filters */
DECLARE_UNIQUE_INDEX(gs_auditing_policy_filters_oid_index, 9541, on gs_auditing_policy_filters using btree(oid oid_ops));
#define GsAuditingPolicyFiltersOidIndexId 9541
/* Add index for policy oid filter row */
DECLARE_UNIQUE_INDEX(gs_auditing_policy_filters_row_index, 9542, on gs_auditing_policy_filters using btree(policyoid oid_ops));
#define GsAuditingPolicyFiltersPolicyOidIndexId 9542

/* Add index of table oid for gs_masking_policy */
DECLARE_UNIQUE_INDEX(gs_masking_policy_oid_index, 9611, on gs_masking_policy using btree(oid oid_ops));
#define GsMaskingPolicyOidIndexId  9611
DECLARE_UNIQUE_INDEX(gs_masking_policy_name_index, 9612, on gs_masking_policy using btree(polname name_ops));
#define GsMaskingPolicyNameIndexId 9612

/* Add index for table oid for masking policy filters */
DECLARE_UNIQUE_INDEX(gs_masking_policy_filters_oid_index, 9641, on gs_masking_policy_filters using btree(oid oid_ops));
#define GsMaskingPolicyFiltersOidIndexId 9641
/* Add index for whole masking policy filter row */
DECLARE_UNIQUE_INDEX(gs_masking_policy_filters_row_index, 9642, on gs_masking_policy_filters using btree(policyoid oid_ops));
#define GsMaskingPolicyFiltersPolicyOidIndexId 9642

/* Add index of table oid for gs_masking_policy_actions */
DECLARE_UNIQUE_INDEX(gs_masking_policy_actions_oid_index, 9651, on gs_masking_policy_actions using btree(oid oid_ops));
#define GsMaskingPolicyActionsOidIndexId 9651

/* Add index for whole masking actions row */
DECLARE_UNIQUE_INDEX(gs_masking_policy_actions_row_index, 9652, on gs_masking_policy_actions using btree(actiontype name_ops, actlabelname name_ops, policyoid oid_ops));
#define GsMaskingPolicyActionsRowIndexId 9652
/* Add index for whole masking actions row */
DECLARE_INDEX(gs_masking_policy_actions_policy_oid_index, 9653, on gs_masking_policy_actions using btree(policyoid oid_ops));
#define GsMaskingPolicyActionsPolicyOidIndexId 9653

/* Add index of table oid for pg_rlspolicy */
DECLARE_UNIQUE_INDEX(pg_rlspolicy_oid_index, 3224, on pg_rlspolicy using btree(oid oid_ops));
#define PgRlspolicyOidIndex	3224
DECLARE_UNIQUE_INDEX(pg_rlspolicy_polrelid_polname_index, 3225, on pg_rlspolicy using btree(polrelid oid_ops, polname name_ops));
#define PgRlspolicyPolrelidPolnameIndex	3225

/* Add index for pg_objcet */
DECLARE_UNIQUE_INDEX(pg_object_index, 3227, on pg_object using btree(object_oid oid_ops, object_type char_ops));
#define PgObjectIndex	3227

/* Add index of table oid for pg_synonym */
DECLARE_UNIQUE_INDEX(pg_synonym_name_nsp_index, 3547, on pg_synonym using btree(synname name_ops, synnamespace oid_ops));
#define SynonymNameNspIndexId  3547
DECLARE_UNIQUE_INDEX(pg_synonym_oid_index, 3548, on pg_synonym using btree(oid oid_ops));
#define SynonymOidIndexId  3548

/* Add index of table oid for streaming_stream, streaming_cont_query */
DECLARE_UNIQUE_INDEX(streaming_stream_oid_index, 3228, on streaming_stream using btree(oid oid_ops));
#define StreamingStreamOidIndexId 3228
DECLARE_UNIQUE_INDEX(streaming_stream_relid_index, 3229, on streaming_stream using btree(relid oid_ops));
#define StreamingStreamRelidIndexId 3229

DECLARE_UNIQUE_INDEX(streaming_cont_query_relid_index, 3230, on streaming_cont_query using btree(relid oid_ops));
#define StreamingContQueryRelidIndexId 3230
DECLARE_UNIQUE_INDEX(streaming_cont_query_defrelid_index, 3231, on streaming_cont_query using btree(defrelid oid_ops));
#define StreamingContQueryDefrelidIndexId 3231
DECLARE_UNIQUE_INDEX(streaming_cont_query_id_index, 3232, on streaming_cont_query using btree(id int4_ops));
#define StreamingContQueryIdIndexId 3232
DECLARE_UNIQUE_INDEX(streaming_cont_query_oid_index, 3233, on streaming_cont_query using btree(oid oid_ops));
#define StreamingContQueryOidIndexId 3233
DECLARE_INDEX(streaming_cont_query_matrelid_index, 3234, on streaming_cont_query using btree(matrelid oid_ops));
#define StreamingContQueryMatrelidIndexId 3234
DECLARE_INDEX(streaming_cont_query_lookupidxid_index, 3235, on streaming_cont_query using btree(lookupidxid oid_ops));
#define StreamingContQueryLookupidxidIndexId 3235
DECLARE_INDEX(streaming_cont_query_schema_change_index, 3236, on streaming_cont_query using btree(matrelid oid_ops, active bool_ops));
#define StreamingContQuerySchemaChangeIndexId 3236
DECLARE_INDEX(streaming_gather_agg_index, 3237, on pg_aggregate using btree(aggtransfn oid_ops, aggcollectfn oid_ops, aggfinalfn oid_ops));
#define StreamingGatherAggIndexId 3237

/* Add index of contquery oid for streaming_reaper_status */
DECLARE_UNIQUE_INDEX(streaming_reaper_status_id_index, 9995, on streaming_reaper_status using btree(id int4_ops));
#define StreamingReaperStatusOidIndexId 9995
DECLARE_UNIQUE_INDEX(streaming_reaper_status_oid_index, 9999, on streaming_reaper_status using btree(oid oid_ops));
#define StreamingCQReaperStatusOidIndexId 9999

/* Add index of table oid for gs_encrypted_columns */
DECLARE_UNIQUE_INDEX(gs_encrypted_columns_oid_index, 9701, on gs_encrypted_columns using btree(oid oid_ops));
#define GsSecEncryptedColumnsOidIndexId  9701
DECLARE_UNIQUE_INDEX(gs_encrypted_columns_rel_id_column_name_index, 9702, on gs_encrypted_columns using btree(rel_id oid_ops, column_name name_ops));
#define GsSecEncryptedColumnsRelidColumnnameIndexId  9702

/* Add index of table oid for gs_client_global_keys */
DECLARE_UNIQUE_INDEX(gs_client_global_keys_oid_index, 9711, on gs_client_global_keys using btree(oid oid_ops));
#define ClientLogicGlobalSettingsOidIndexId  9711
DECLARE_UNIQUE_INDEX(gs_client_global_keys_name_index, 9712, on gs_client_global_keys using btree(global_key_name name_ops, key_namespace oid_ops));
#define ClientLogicGlobalSettingsNameIndexId  9712

/* Add index of table oid for gs_column_keys */
DECLARE_UNIQUE_INDEX(gs_column_keys_oid_index, 9721, on gs_column_keys using btree(oid oid_ops));
#define ClientLogicColumnSettingsOidIndexId  9721
DECLARE_UNIQUE_INDEX(gs_column_keys_name_index, 9722, on gs_column_keys using btree(column_key_name name_ops, key_namespace oid_ops));
#define ClientLogicColumnSettingsNameIndexId  9722
DECLARE_UNIQUE_INDEX(gs_column_keys_distributed_id_index, 9723, on gs_column_keys using btree(column_key_distributed_id oid_ops));
#define ClientLogicColumnSettingDistributedIdIndexId  9723

/* Add index of table oid for gs_client_global_keys_args */
DECLARE_UNIQUE_INDEX(gs_client_global_keys_args_oid_index, 9731, on gs_client_global_keys_args using btree(oid oid_ops));
#define ClientLogicGlobalSettingsArgsOidIndexId  9731

/* Add index of table oid for gs_sec_column_encrption_keys_args */
DECLARE_UNIQUE_INDEX(gs_column_keys_args_oid_index, 9741, on gs_column_keys_args using btree(oid oid_ops));
#define ClientLogicColumnSettingsArgsOidIndexId  9741

/* Add index of table oid for gs_encrypted_proc */
DECLARE_UNIQUE_INDEX(gs_encrypted_proc_oid, 9751, on gs_encrypted_proc using btree(oid oid_ops));
#define GsClProcOid  9751
DECLARE_UNIQUE_INDEX(gs_encrypted_proc_func_id_index, 9752, on gs_encrypted_proc using btree(func_id oid_ops));
#define GsClProcFuncIdIndexId  9752

/* Add index for matview catalog*/
DECLARE_UNIQUE_INDEX(gs_matview_oid_index, 9991, on gs_matview using btree(oid oid_ops));
#define GsMatviewOidIndexId 9991
DECLARE_UNIQUE_INDEX(gs_matviewdep_oid_index, 9992, on gs_matview_dependency using btree(oid oid_ops));
#define GsMatviewDepOidIndexId 9992

/* Add indexes for pg_recyclebin */
DECLARE_UNIQUE_INDEX(gs_recyclebin_id_index, 8647, on gs_recyclebin using btree(oid oid_ops));
#define RecyclebinIdIndexId  8647
DECLARE_INDEX(gs_recyclebin_baseid_index, 8648, on gs_recyclebin using btree(rcybaseid oid_ops));
#define RecyclebinBaseidIndexId  8648
DECLARE_INDEX(gs_recyclebin_name_index, 8649, on gs_recyclebin using btree(rcyname name_ops));
#define RecyclebinNameIndexId  8649
DECLARE_INDEX(gs_recyclebin_dbid_nsp_oriname_index, 8650, on gs_recyclebin using btree(rcynamespace oid_ops, rcydbid oid_ops, rcyoriginname name_ops, rcyrecyclecsn int8_ops));
#define RecyclebinDbidNspOrinameIndexId  8650
DECLARE_INDEX(gs_recyclebin_dbid_spcid_rcycsn_index, 8651, on gs_recyclebin using btree(rcytablespace oid_ops, rcydbid oid_ops, rcyrecyclecsn int8_ops));
#define RecyclebinDbidSpcidRcycsnIndexId  8651
DECLARE_INDEX(gs_recyclebin_dbid_relid_index, 8652, on gs_recyclebin using btree(rcydbid oid_ops, rcyrelid oid_ops));
#define RecyclebinDbidRelidIndexId  8652

/* Add indexes for pg_snapshot */
DECLARE_INDEX(gs_txn_snapshot_time_csn_index, 8653, on gs_txn_snapshot using btree(snptime timestamptz_ops desc, snpcsn int8_ops asc));
#define SnapshotTimeCsnIndexId  8653
DECLARE_INDEX(gs_txn_snapshot_csn_xmin_index, 8654, on gs_txn_snapshot using btree(snpcsn int8_ops desc, snpxmin int8_ops asc));
#define SnapshotCsnXminIndexId  8654
DECLARE_INDEX(gs_txn_snapshot_xmin_index, 8655, on gs_txn_snapshot using btree(snpxmin int8_ops));
#define SnapshotXminIndexId  8655

/* Add index of table model name for gs_opt_model */
DECLARE_UNIQUE_INDEX(gs_opt_model_name_index, 9997, on gs_opt_model using btree(model_name name_ops));
#define GsOPTModelNameIndexId	9997

DECLARE_UNIQUE_INDEX(gs_model_oid_index, 3992, on gs_model_warehouse using btree(oid oid_ops));
#define GsModelOidIndexId    3992
DECLARE_UNIQUE_INDEX(gs_model_name_index, 3993, on gs_model_warehouse using btree(modelname name_ops));
#define GsModelNameIndexId   3993

DECLARE_UNIQUE_INDEX(pg_publication_oid_index, 6120, on pg_publication using btree(oid oid_ops));
#define PublicationObjectIndexId 6120

DECLARE_UNIQUE_INDEX(pg_publication_pubname_index, 6121, on pg_publication using btree(pubname name_ops));
#define PublicationNameIndexId 6121

DECLARE_UNIQUE_INDEX(pg_publication_rel_oid_index, 6122, on pg_publication_rel using btree(oid oid_ops));
#define PublicationRelObjectIndexId 6122

DECLARE_UNIQUE_INDEX(pg_publication_rel_map_index, 6123, on pg_publication_rel using btree(prrelid oid_ops, prpubid oid_ops));
#define PublicationRelMapIndexId 6123

DECLARE_UNIQUE_INDEX(pg_subscription_oid_index, 6124, on pg_subscription using btree(oid oid_ops));
#define SubscriptionObjectIndexId 6124

DECLARE_UNIQUE_INDEX(pg_subscription_subname_index, 6125, on pg_subscription using btree(subdbid oid_ops, subname name_ops));
#define SubscriptionNameIndexId 6125

DECLARE_UNIQUE_INDEX(pg_replication_origin_roident_index, 6136, on pg_replication_origin using btree(roident oid_ops));
#define ReplicationOriginIdentIndex 6136

DECLARE_UNIQUE_INDEX(pg_replication_origin_roname_index, 6137, on pg_replication_origin using btree(roname text_pattern_ops));
#define ReplicationOriginNameIndex 6137


/* last step of initialization script: build the indexes declared above */
BUILD_INDICES

#endif   /* INDEXING_H */
