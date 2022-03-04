/* -------------------------------------------------------------------------
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * cached_proc.h
 *
 * IDENTIFICATION
 *   src\common\interfaces\libpq\client_logic_cache\cached_proc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_PROC_H
#define CACHED_PROC_H

#include <algorithm>
#include "cached_column.h"
#include "icached_rec.h"
/*
 * select func_id, proargcachedcol, proallargtypes_orig,
 * proname,pronargs,proargtypes,proallargtypes,proargnames,nspname   from gs_encrypted_proc gs_proc join pg_proc on
 * gs_proc.func_id = pg_proc.oid join pg_namespace ON (pg_namespace.oid = pronamespace);
 */
class CachedProc : public ICachedRec {
public:
    CachedProc(PGconn* const conn)
        : m_conn(conn),
          m_func_id(0),
          m_proname(NULL),
          m_pronargs(0),
          m_proargcachedcol(NULL),
          m_proargtypes(NULL),
          m_nallargtypes(0),
          m_proallargtypes(NULL),
          m_proallargtypes_orig(NULL),
          m_nargnames(0),
          m_proargnames(NULL),
          m_proargmodes(NULL),
          m_schema_name(NULL),
          m_dbname(NULL),
          m_refcount(0) {};

    ~CachedProc() override;
    /*
     * whether the list is ordered according to the original table order or
     * is accorded according to the current DML query 
     */
    const Oid get_original_id(const size_t idx) const override;
    void set_original_ids();
    const size_t get_num_processed_args() const override
    {
        return m_nallargtypes;
    }
    PGconn* const m_conn;
    Oid m_func_id;
    char* m_proname;
    int m_pronargs;
    Oid* m_proargcachedcol;
    Oid* m_proargtypes;
    size_t m_nallargtypes;
    Oid* m_proallargtypes;
    Oid* m_proallargtypes_orig;
    size_t m_nargnames;
    char** m_proargnames;
    char* m_proargmodes; /*argmod of param might be OUT, INOUT, IN, VARIADIC or TABLE*/
    char* m_schema_name;
    char* m_dbname;
    size_t m_refcount;
};

#endif
