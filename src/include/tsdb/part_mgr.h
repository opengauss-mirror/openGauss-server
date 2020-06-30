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
 * ---------------------------------------------------------------------------------------
 *
 * part_mgr.h
 *      manager of parts
 *
 * IDENTIFICATION
 *        src/include/tsdb/part_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TSDB_PART_MGR_H
#define TSDB_PART_MGR_H

#include "utils/relcache.h"
#include "tsdb/constant_def.h"

class DataRow;
class Part;

class PartMgr:public BaseObject {
public:
    PartMgr(Relation relation, Oid partition_oid);
    bool load_parts();
private:
    char* generate_magic_name();
    void add_part(Part *part);
    List* findDesctableOid();
private:
    Relation relation;
    Oid partition_oid;
    Part *parts;
    int nums;
};

#endif /* TSDB_PART_MGR_H */

