#This is the main CMAKE for build all components.
set(POSTGRES_BKI_SRCS_S 
"@pg_proc.h @pg_type.h @pg_attribute.h @pg_class.h @pg_partition.h @pg_attrdef.h @pg_constraint.h
@pg_inherits.h @pg_index.h @pg_operator.data @pg_opfamily.h @pg_opclass.h @pg_am.h @pg_amop.data @pg_amproc.h
@pg_language.h @pg_largeobject_metadata.h @pg_largeobject.h @pg_aggregate.h @pg_statistic.h @pg_rewrite.h
@pg_trigger.h @pg_description.h @pg_cast.h @pg_enum.h @pg_namespace.h @pg_conversion.h @pg_depend.h @pg_database.h
@pg_db_role_setting.h @pg_tablespace.h @pg_pltemplate.h @pg_authid.h @pg_auth_members.h @pg_shdepend.h @pg_shdescription.h
@pg_ts_config.h @pg_ts_config_map.h @pg_ts_dict.h @pg_ts_parser.h @pg_ts_template.h @pg_auth_history.h @pg_user_status.h
@pg_extension.h @pg_obsscaninfo.h @pg_foreign_data_wrapper.h @pg_foreign_server.h @pg_user_mapping.h @pgxc_class.h @pgxc_node.h
@pgxc_group.h @pg_resource_pool.h @pg_workload_group.h @pg_app_workloadgroup_mapping.h @pg_foreign_table.h @pg_rlspolicy.h
@pg_default_acl.h @pg_seclabel.h @pg_shseclabel.h @pg_collation.h @pg_range.h @gs_policy_label.h @gs_auditing_policy.h
@gs_auditing_policy_acc.h @gs_auditing_policy_priv.h @gs_auditing_policy_filter.h @gs_masking_policy.h @gs_masking_policy_actions.h
@gs_masking_policy_filters.h @gs_encrypted_columns.h @gs_column_keys.h @gs_column_keys_args.h @gs_client_global_keys.h @gs_encrypted_proc.h
@gs_client_global_keys_args.h @pg_job.h @gs_asp.h @pg_job_proc.h @pg_extension_data_source.h @pg_statistic_ext.h
@pg_object.h @pg_synonym.h @toasting.h @indexing.h @gs_obsscaninfo.h @pg_directory.h @pg_hashbucket.h @gs_global_chain.h @gs_global_config.h
@pg_streaming_stream.h @pg_streaming_cont_query.h @pg_streaming_reaper_status.h @gs_matview.h @gs_matview_dependency.h @pgxc_slice.h 
@gs_opt_model.h @pg_recyclebin.h @pg_snapshot.h @gs_model.h @gs_package.h @gs_job_argument.h @gs_job_attribute.h @pg_uid.h @gs_db_privilege.h
@pg_replication_origin.h @pg_publication.h @pg_publication_rel.h @pg_subscription.h"

)

string(REPLACE "@" "${PROJECT_SRC_DIR}/include/catalog/" POSTGRES_BKI_SRCS ${POSTGRES_BKI_SRCS_S})

set(INCLUDE_PATH -I${PROJECT_SRC_DIR}/include/catalog)

set(catalog_perl "${PROJECT_SRC_DIR}/common/backend/catalog|||perl -I ${PROJECT_SRC_DIR}/common/backend/catalog genbki.pl ${INCLUDE_PATH} --set-version=9.2 ${POSTGRES_BKI_SRCS}|")
add_cmd_gen_when_configure(perl_target catalog_perl)

execute_process(
    COMMAND ln -fs ${CMAKE_CURRENT_SOURCE_DIR}/schemapg.h  ${PROJECT_SRC_DIR}/include/catalog/schemapg.h
)

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_catalog_SRC)
set(TGT_catalog_INC 
    ${PROJECT_OPENGS_DIR}/contrib/log_fdw
    ${PROJECT_TRUNK_DIR}/distribute/bin/gds
    ${PROJECT_SRC_DIR}/include/libcomm
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/lib/gstrace
    ${LZ4_INCLUDE_PATH}
    ${LIBCGROUP_INCLUDE_PATH}
    ${LIBORC_INCLUDE_PATH}
    ${EVENT_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_PATH}
    ${ZLIB_INCLUDE_PATH}
)

set(catalog_DEF_OPTIONS ${MACRO_OPTIONS})
set(catalog_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(catalog_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(common_backend_catalog TGT_catalog_SRC TGT_catalog_INC "${catalog_DEF_OPTIONS}" "${catalog_COMPILE_OPTIONS}" "${catalog_LINK_OPTIONS}")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgres.bki
    DESTINATION share/postgresql/
) 
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgres.description
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/postgres.shdescription
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/system_views.sql
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/performance_views.sql
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/information_schema.sql
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/sql_features.txt
    DESTINATION share/postgresql/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/pg_cast_oid.txt
    DESTINATION share/postgresql/
)

if("${ENABLE_MULTIPLE_NODES}_${ENABLE_PRIVATEGAUSS}" STREQUAL "OFF_ON")
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../privategauss/kernel/catalog/private_system_views.sql
    DESTINATION share/postgresql/
)
endif()


