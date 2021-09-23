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
 * column_hook_executor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\column_hook_executor.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef COLUMN_HOOK_EXECUTOR_H
#define COLUMN_HOOK_EXECUTOR_H

#include <string>
#include "abstract_hook_executor.h"
#include "client_logic/client_logic_enums.h"
#include "client_logic_cache/icached_column.h"
#include "nodes/parsenodes_common.h"
#include "client_logic_processor/values_processor.h"
typedef unsigned int Oid;

class PGClientLogic;
class GlobalHookExecutor;
class CStringsMap;
typedef CStringsMap StringArgs;

/*
 * Every Column Setting object holds a ColumnHookExecutor instance.
 * The ColumnHookExecutor is an abstract class. The actual class to be initiated is based on the type chosen in
 * creation.
 */

class ColumnHookExecutor : public AbstractHookExecutor {
public:
    ColumnHookExecutor(GlobalHookExecutor *global_hook_executor, Oid oid, const char *function_name);
    virtual ~ColumnHookExecutor();
    /* REQUIRED INTERFACES */

    /*
     * When a query is sent to the server, the data in the query first needs to be processed before the query is sent.
     * The query is replaced with the modified query upon submission.
     */
    virtual int process_data_impl(const ICachedColumn *cached_column,
        const unsigned char *data, int data_size, unsigned char *processed_data) = 0;

    /*
     * returns an estimation of the size of the buffer that needs to be allocated for the processed_data argument
     */
    virtual int get_estimated_processed_data_size_impl(int data_size) const = 0;

    /*
     * When the client receives the response from the server, it has to be (de)processed.
     * The data is deprocessed to its original form in the client side so it can be transffered to the application.
     */
    virtual DecryptDataRes deprocess_data_impl(const unsigned char *data_processed,
        int data_processed_size, unsigned char **data, int *data_plain_size) = 0;

    /*
     * returns the name of the data type the column was converted to.
     * in run-time the column is converted to the original data type chosen by the user before the result set is sent
     * to the application.
     */
    virtual const char *get_data_type(const ColumnDef * const column) = 0;
    virtual void set_data_type(const ColumnDef * const column, ICachedColumn *ce) = 0;

    /*
     * OPTIONAL INTERFACES
     */
    virtual bool pre_create(PGClientLogic &client_logic, const StringArgs &args, StringArgs &new_args);
    virtual bool is_set_operation_allowed(const ICachedColumn *cached_column) const;
    virtual bool is_operator_allowed(const ICachedColumn *ce, const char * const op) const;

    /* BUILT-IN FUNCTIONS */
    const Oid getOid() const;

    /* wrapper functions for impl functions */
    int process_data(const ICachedColumn *cached_column, const unsigned char *data, int data_size,
        unsigned char *processed_data);
    DecryptDataRes deprocess_data(const unsigned char *processed_data, int processed_data_size,
        unsigned char **data, int *data_plain_size);
    int get_estimated_processed_data_size(int data_size) const;
    virtual bool set_deletion_expected();
    void inc_ref_count() override;
    void dec_ref_count() override;
    GlobalHookExecutor *get_global_hook_executor() const;

    GlobalHookExecutor *m_global_hook_executor;
protected:
    Oid m_oid;
};

#endif
