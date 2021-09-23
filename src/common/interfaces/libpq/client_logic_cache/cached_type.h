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
 * cached_type.h
 *
 * IDENTIFICATION
 *   src\common\interfaces\libpq\client_logic_cache\cached_type.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_TYPE_H
#define CACHED_TYPE_H

#include "libpq/libpq-int.h"
#include "icached_rec.h"
#include "icached_columns.h"
class ICachedColumnManager; 

class CachedType : public ICachedRec {
public:
    CachedType(const Oid typid, const char* m_typname, const char* schema_name, const char* dbname,
        const ICachedColumnManager* cached_column_manager);
    void init();
    ~CachedType() override;
    const bool get_type_columns(ICachedColumns* cached_columns) const;
    const Oid get_oid() const
    {
        return m_typid;
    }
    const Oid get_original_id(const size_t idx) const override;
    const size_t get_num_processed_args() const override
    {
        return m_cached_columns->size();
    }

private:
    void init(const char* typname, const char* schema_name, const char* dbname);
    const Oid m_typid;
    char* m_typname;
    char* m_schema_name;
    char* m_dbname;
    const ICachedColumnManager* m_cached_column_manager;
    ICachedColumns* m_cached_columns;
};

#endif