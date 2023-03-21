/*
 * dblink.h
 *
 * Functions returning results from a remote database
 *
 * Joe Conway <mail@joeconway.com>
 * And contributors:
 * Darko Prenosil <Darko.Prenosil@finteh.hr>
 * Shridhar Daithankar <shridhar_daithankar@persistent.co.in>
 *
 * contrib/dblink/dblink.h
 * Copyright (c) 2001-2012, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

#ifndef DBLINK_H
#define DBLINK_H

#include "fmgr.h"
#include "sql.h"
#include "sqlext.h"


typedef struct ODBCconn {
    SQLHENV       envHandle;        /* Handle ODBC environment */
    SQLHDBC       connHandle;       /* Handle connection */
    SQLHSTMT      stmt;             /* Handle sql */
} ODBCconn;

typedef struct storeInfo {
    FunctionCallInfo fcinfo;
    Tuplestorestate* tuplestore;
    AttInMetadata* attinmeta;
    MemoryContext tmpcontext;
    char** cstrs;
    /* temp storage for results to avoid leaks on exception */
    PGresult* last_res;
    PGresult* cur_res;
} storeInfo;

typedef struct LinkInfo {
    SQLCHAR* username;      /* odbc username */
    SQLCHAR* password;      /* odbc password */
    SQLCHAR* drivername;    /* odbc driver name */
} LinkInfo;

class Linker : public BaseObject {
public:
    Linker();
    virtual void finish() = 0;
    virtual text* exec(char* conname, const char* sql, bool fail) = 0;
    virtual char* errorMsg() = 0;
    virtual int isBusy() = 0;
    virtual char* cancel(PGcancel* cancel) = 0;
    virtual int sendQuery(char* sql) = 0;
    virtual char* open(char* conname, char* sql, bool fail) = 0;
    virtual char* close(char* conname, char* sql, bool fail) = 0;
    virtual void getResult(char* conname, FunctionCallInfo fcinfo, char* sql, bool fail) = 0;
    virtual void queryResult(ReturnSetInfo* rsinfo, const char* conname, storeInfo* sinfo, const char* sql, bool fail) = 0;
    virtual void fetch(char* conname, FunctionCallInfo fcinfo, const char* sql, bool fail, char* curname) = 0;
    virtual void getNotify(ReturnSetInfo* rsinfo) = 0;
};

typedef struct remoteConn {
    Linker* linker;
} remoteConn;

typedef struct dblink_session_context {
    remoteConn* pconn;
    HTAB* remoteConnHash;
    bool needFree;
} dblink_session_context;

/*
 *	Following is list that holds multiple remote connections.
 *	Calling convention of each dblink function changes to accept
 *	connection name as the first parameter. The connection list is
 *	much like ecpg e.g. a mapping between a name and a PGconn object.
 */

typedef struct remoteConnHashEnt {
    char name[NAMEDATALEN];
    remoteConn* rconn;
} remoteConnHashEnt;

class PQLinker : public Linker {
public: 
    PGconn* conn;
    PGresult* res;
    int openCursorCount;        /* The number of open cursors */
    bool newXactForCursor;      /* Opened a transaction for a cursor */
public:
    PQLinker(char* connstr);
    void finish();
    text* exec(char* conname, const char* sql, bool fail);
    char* errorMsg();
    int isBusy();
    char* cancel(PGcancel* cancel);
    int sendQuery(char* sql);
    char* open(char* conname, char* sql, bool fail);
    char* close(char* conname, char* sql, bool fail);
    void getResult(char* conname, FunctionCallInfo fcinfo, char* sql, bool fail);
    void queryResult(ReturnSetInfo* rsinfo, const char* conname, storeInfo* sinfo, const char* sql, bool fail);
    void fetch(char* conname, FunctionCallInfo fcinfo, const char* sql, bool fail, char* curname);
    void getNotify(ReturnSetInfo* rsinfo);
};

class ODBCLinker : public Linker {
public:
    SQLHENV       envHandle;        /* Handle ODBC environment */
    SQLHDBC       connHandle;       /* Handle connection */
    SQLHSTMT      stmt;             /* Handle sql */
    char*         msg;              /* error message */
public:
    ODBCLinker(char* connstr_or_name);
    void finish();
    text* exec(char* conname, const char* sql, bool fail);
    char* errorMsg();
    int isBusy();
    char* cancel(PGcancel* cancel);
    int sendQuery(char* sql);
    char* open(char* conname, char* sql, bool fail);
    char* close(char* conname, char* sql, bool fail);
    void getResult(char* conname, FunctionCallInfo fcinfo, char* sql, bool fail);
    void queryResult(ReturnSetInfo* rsinfo, const char* conname, storeInfo* sinfo, const char* sql, bool fail);
    void fetch(char* conname, FunctionCallInfo fcinfo, const char* sql, bool fail, char* curname);
    void getNotify(ReturnSetInfo* rsinfo);
};

/*
 * External declarations
 */
extern "C" Datum dblink_connect(PG_FUNCTION_ARGS);
extern "C" Datum dblink_disconnect(PG_FUNCTION_ARGS);
extern "C" Datum dblink_open(PG_FUNCTION_ARGS);
extern "C" Datum dblink_close(PG_FUNCTION_ARGS);
extern "C" Datum dblink_fetch(PG_FUNCTION_ARGS);
extern "C" Datum dblink_record(PG_FUNCTION_ARGS);
extern "C" Datum dblink_send_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_result(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_connections(PG_FUNCTION_ARGS);
extern "C" Datum dblink_is_busy(PG_FUNCTION_ARGS);
extern "C" Datum dblink_cancel_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_error_message(PG_FUNCTION_ARGS);
extern "C" Datum dblink_exec(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_pkey(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_insert(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_delete(PG_FUNCTION_ARGS);
extern "C" Datum dblink_build_sql_update(PG_FUNCTION_ARGS);
extern "C" Datum dblink_current_query(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_notify(PG_FUNCTION_ARGS);
extern "C" Datum dblink_get_drivername(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);

#endif /* DBLINK_H */
