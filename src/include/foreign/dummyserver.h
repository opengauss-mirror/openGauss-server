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
 * dummyserver.h
 *        support for computing pool. The dummy server is used to store the
 *        connection information of computing pool.
 * 
 * 
 * IDENTIFICATION
 *        src/include/foreign/dummyserver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GAUSSDB_FOREIGN_DUMMYSERVER_H
#define GAUSSDB_FOREIGN_DUMMYSERVER_H

#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/hsearch.h"

#ifndef DUMMY_SERVER
#define DUMMY_SERVER "dummy"
#endif

/* user name for logging in computing pool. */
#define OPTION_NAME_USER_NAME "username"
/* password for logging in computing pool. */
#define OPTION_NAME_PASSWD "password"

char* getOptionValueFromOptionList(List* optionList, const char* optionName);

/*
 * Store computing pool information in order to connect
 * computing pool.
 */
typedef struct DummyServerOptions {
    /* the given dummy server oid. it is used to be hash key also.
     the server information will store in cache. */
    Oid serverOid;
    /*user name of connect computing pool. */
    char* userName;
    /* user password of connect computing pool. */
    char* passWord;
    char* address;
    char* dbname;
    char* remoteservername;
} DummyServerOptions;

/**
 * @Description: Get the dummy server options. Currently, only dummy server
 * exist in database.
 * @return return server options if found it, otherwise return NULL.
 */
DummyServerOptions* getDummyServerOption();

/**
 * @Description: Init dummy server cache.
 * @return none.
 */
void InitDummyServrCache();

/**
 * @Description: Drop cache when we operator alter server
 * or drop server.
 * @in serverOid, the server oid to be deleted.
 * @return none.
 */
void InvalidDummyServerCache(Oid serverOid);

/**
 * @Description: get dummy server oid from catalog.
 * @return return server oid if find one server oid, otherwise return InvalidOid.
 */
Oid getDummyServerOid();

/**
 * @Description: if the optionList belong to dummy server, we need check the entire
 * database. if exists one dummy server, throw error, otherwise do nothing.
 * @in optionList, the option list to be checked.
 * @return if exists one dummy server in current database,
 * throw error, otherwise do nothing.
 */

void checkExistDummyServer(List* optionList);

/**
 * @Description: jugde a dummy server type by checking server option list.
 * @in optionList, the option list to be checked.
 * @return
 */
bool isDummyServerByOptions(List* optionList);

#endif