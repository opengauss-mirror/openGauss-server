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
 * cached_column.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_column.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_COLUMN_H
#define CACHED_COLUMN_H

/*
 * Data structure being used for saving column information in cache
 * all of the columns configured with a client logic policy have this object
 */

#include "icached_column.h"

typedef unsigned int Oid;

class colFullName;
class ColumnHookExecutor;
class ColumnHookExecutorsList;

class CachedColumn : public ICachedColumn {
public:
    /* *
     * @brief Default Constructor, in use for case not all info is exist in time of object creation
     */
    CachedColumn(Oid oid, Oid table_oid, const char *database_name, const char *schema_name, const char *table_name,
        const char *column_name, int column_position, Oid data_type_oid, int data_type_mod);

    /*
     * fake column for CREATE TABLE
     */
    CachedColumn();
    explicit CachedColumn(const ICachedColumn *source);
    void init();
    void init(const ICachedColumn *source);
    /* *
     * @brief Destructor
     */
    ~CachedColumn();
    /* *
     * @return ordinal position of column in table
     */
    unsigned int get_col_idx() const override;

    /* *
     * @return column name without prefix and with prefix (fqdn)
     */
    const char *get_catalog_name() const override;
    const char *get_col_name() const override;
    const char *get_schema_name() const override;
    const char *get_table_name() const override;
    const Oid get_table_oid() const override;

    ColumnHookExecutorsList *get_column_hook_executors() const override;
    Oid get_origdatatype_oid() const override;
    int get_origdatatype_mod() const override;

    /*
     * required for fake column for CREATE TABLE
     */
    void set_origdatatype_oid(Oid)override;

public:
    /* *
     * @return full qualified name of the column, using pattern: DB.Schmea.Table.Column
     */

    /* *
     * @param Database name
     */
    void set_catalog_name(const char *catalog_name) override;

    /* *
     * @param  column name only, without table prefix
     */
    void set_col_name(const char *col_name) override;

    /* *
     * @param  schema name only, without database prefix
     */
    void set_schema_name(const char *schema_name) override;

    /* *
     * @param table name only, without schema prefix
     */
    void set_table_name(const char *table_name) override;

    void set_table_oid(Oid table_oid);

    void set_data_type(Oid data_type) override;

    Oid get_data_type() const override;

    /* *
     * adding executor to cacehd column
     * @param
     */
    void add_executor(ColumnHookExecutor *executor);

    void set_use_in_prepare(bool) override;
    const bool is_in_prepare() const override;
    void remove_from_columnslist();
    void set_flag_columnslist();
    const bool is_in_columns_list() const override;
    ColumnHookExecutorsList *m_column_hook_executors_list; /* Opaque Arguments pass to hook */
     
    Oid get_oid() const;
    void set_oid(Oid oid);

private:
    Oid m_own_oid;               /* column oid - used by gs_encrypted_proc to relate input parameter */
    Oid m_table_oid;             /* < table oid - being used as one of the options to identify column */
    unsigned int m_column_index; /* < A value that uniquely identifies this column within its table */
    Oid m_data_type;
    Oid m_data_type_original_oid; /* Original type Oid of the column - used for binary conversion before processing */
    int m_data_type_original_mod; /* Original type Oid of the column - used for validating */
    colFullName *m_col_full_name;   /* struct to save FQDN of this column */
    bool m_in_columns_list;                                /* is this column in columns_list */
    size_t in_use_in_prepare;
};

#endif
