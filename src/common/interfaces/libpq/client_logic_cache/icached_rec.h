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
 * icached_rec.h
 *
 * IDENTIFICATION
 *   src\common\interfaces\libpq\client_logic_cache\icached_rec.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ICACHED_REC_H
#define ICACHED_REC_H

#include "libpq/libpq-fe.h"

class ICachedRec {
public:
    ICachedRec(): m_original_ids(NULL){}
    virtual ~ICachedRec()
    {
        if (m_original_ids) {
            delete m_original_ids;
            m_original_ids = NULL;
        }
    }
    virtual const int* get_original_ids() const 
    {
        return m_original_ids;
    }
    virtual const size_t get_num_processed_args() const =0;
protected:
    virtual const Oid get_original_id(const size_t idx) const =0;
    int* m_original_ids;
};

#endif