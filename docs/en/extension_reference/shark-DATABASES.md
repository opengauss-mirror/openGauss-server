# DATABASES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:00.921Z pushedAt=2026-09-21T10:00:37.810Z -->

The DATABASES view returns database information.

**Table 1** DATABASES

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>name</td>
            <td>name</td>
            <td>Database name</td>
        </tr>
        <tr>
            <td>database_id</td>
            <td>int</td>
            <td>Database ID</td>
        </tr>
        <tr>
            <td>source_database_id</td>
            <td>int</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>owner_sid</td>
            <td>oid</td>
            <td>Owner ID</td>
        </tr>
        <tr>
            <td>create_date</td>
            <td>timestamp</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>compatibility_level</td>
            <td>tinyint</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>collation_name</td>
            <td>name</td>
            <td>Collation of the database</td>
        </tr>
        <tr>
            <td>user_access</td>
            <td>tinyint</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>user_access_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'MULTI_USER'</td>
        </tr>
        <tr>
            <td>is_read_only</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_auto_close_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_auto_shrink_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>state</td>
            <td>tinyint</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>state_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'ONLINE'</td>
        </tr>
        <tr>
            <td>is_in_standby</td>
            <td>bit</td>
            <td>For restore logs, the database is read-only</td>
        </tr>
        <tr>
            <td>is_cleanly_shutdown</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_supplemental_logging_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>snapshot_isolation_state</td>
            <td>tinyint</td>
            <td>Status of allowing snapshot isolation transactions, returns 1</td>
        </tr>
        <tr>
            <td>snapshot_isolation_state_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'ON'</td>
        </tr>
        <tr>
            <td>is_read_committed_snapshot_on</td>
            <td>bit</td>
            <td>read-committed isolation level, returns 1</td>
        </tr>
        <tr>
            <td>recovery_model</td>
            <td>tinyint</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>recovery_model_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'FULL'</td>
        </tr>
        <tr>
            <td>page_verify_option</td>
            <td>tinyint</td>
            <td>PAGE_VERIFY option setting, returns 0</td>
        </tr>
        <tr>
            <td>page_verify_option_desc</td>
            <td>nvarchar(60)</td>
            <td>Description of the PAGE_VERIFY option setting. Returns NULL</td>
        </tr>
        <tr>
            <td>is_auto_create_stats_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_auto_create_stats_incremental_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_auto_update_stats_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_auto_update_stats_async_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_ansi_null_default_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_ansi_nulls_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_ansi_padding_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_ansi_warnings_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_arithabort_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_concat_null_yields_null_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_numeric_roundabort_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_quoted_identifier_on</td>
            <td>bit</td>
            <td>Returns 1</td>
        </tr>
        <tr>
            <td>is_recursive_triggers_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_cursor_close_on_commit_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_local_cursor_default</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_fulltext_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_trustworthy_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_db_chaining_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_parameterization_forced</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_master_key_encrypted_by_server</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_query_store_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_published</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_subscribed</td>
            <td>bit</td>
            <td>This column is not used. It always returns 0 regardless of the subscription status of the database</td>
        </tr>
        <tr>
            <td>is_merge_published</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_distributor</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_sync_with_backup</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>service_broker_guid</td>
            <td>oid</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>is_broker_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>log_reuse_wait</td>
            <td>tinyint</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>log_reuse_wait_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'NOTHING'</td>
        </tr>
        <tr>
            <td>is_date_correlation_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_cdc_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_encrypted</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>nais_honor_broker_priority_onme</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>replica_id</td>
            <td>oid</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>group_database_id</td>
            <td>oid</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>resource_pool_id</td>
            <td>int</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_language_lcid</td>
            <td>smallint</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_language_name</td>
            <td>nvarchar(128)</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_fulltext_language_lcid</td>
            <td>int</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>default_fulltext_language_name</td>
            <td>nvarchar(128)</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>is_nested_triggers_on</td>
            <td>bit</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>is_transform_noise_words_on</td>
            <td>bit</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>two_digit_year_cutoff</td>
            <td>smallint</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>containment</td>
            <td>tinyint</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>containment_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'NONE'</td>
        </tr>
        <tr>
            <td>target_recovery_time_in_seconds</td>
            <td>int</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>delayed_durability</td>
            <td>int</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>delayed_durability_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>is_memory_optimized_elevate_to_snapshot_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_federation_member</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_remote_data_archive_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_mixed_page_allocation_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_temporal_history_retention_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>catalog_collation_type</td>
            <td>int</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>catalog_collation_type_desc</td>
            <td>nvarchar(60)</td>
            <td>Returns 'Not Application'</td>
        </tr>
        <tr>
            <td>physical_database_name</td>
            <td>nvarchar(128)</td>
            <td>Returns NULL</td>
        </tr>
        <tr>
            <td>is_result_set_caching_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_accelerated_database_recovery_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_tempdb_spill_to_remote_store</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_stale_page_detection_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_memory_optimized_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_data_retention_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_ledger_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_change_feed_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_event_stream_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_vorder_enabled</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
        <tr>
            <td>is_optimized_locking_on</td>
            <td>bit</td>
            <td>Returns 0</td>
        </tr>
    </tbody>
</table>