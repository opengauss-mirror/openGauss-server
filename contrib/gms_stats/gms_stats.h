/*---------------------------------------------------------------------------------------*
 * gms_stats.h
 *
 *  Definition about gms_stats package.
 *
 * IDENTIFICATION
 *        contrib/gms_stats/gms_stats.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_STATS_H
#define GMS_STATS_H

extern "C" Datum gs_analyze_schema_tables(PG_FUNCTION_ARGS);
extern "C" Datum gs_create_stat_table(PG_FUNCTION_ARGS);
extern "C" Datum gs_drop_stat_table(PG_FUNCTION_ARGS);
extern "C" Datum gs_lock_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_lock_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_lock_partition_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_unlock_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_unlock_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_unlock_partition_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_export_column_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_export_index_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_export_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_export_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_import_column_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_import_index_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_import_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_import_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_delete_column_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_delete_index_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_delete_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_delete_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_set_column_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_set_index_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_set_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_restore_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_restore_schema_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_gather_table_stats(PG_FUNCTION_ARGS);
extern "C" Datum gs_gather_database_stats(PG_FUNCTION_ARGS);
extern "C" Datum get_stats_history_availability(PG_FUNCTION_ARGS);
extern "C" Datum get_stats_history_retention(PG_FUNCTION_ARGS);
extern "C" Datum purge_stats(PG_FUNCTION_ARGS);

#define atooid(x) ((Oid)strtoul((x), NULL, 10))
#endif
