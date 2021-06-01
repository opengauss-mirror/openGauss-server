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
 * datasource.h
 *        support for data source
 * 
 * 
 * IDENTIFICATION
 *        src/include/datasource/datasource.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DATASOURCE_H
#define DATASOURCE_H

#include "nodes/parsenodes.h"

typedef struct DataSource {
    Oid sourceid;     /* source Oid */
    Oid owner;        /* source owner user Oid */
    char* srcname;    /* name of the source */
    char* srctype;    /* source type, optional */
    char* srcversion; /* source version, optional */
    List* options;    /* source options as DefElem list */
} DataSource;

extern Oid get_data_source_oid(const char* sourcename, bool missing_ok);
extern DataSource* GetDataSource(Oid sourceid);
extern DataSource* GetDataSourceByName(const char* sourcename, bool missing_ok);

#endif /* DATASOURCE_H */
