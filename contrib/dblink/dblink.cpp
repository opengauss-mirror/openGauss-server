/*
 * dblink.c
 *
 * Functions returning results from a remote database
 *
 * Joe Conway <mail@joeconway.com>
 * And contributors:
 * Darko Prenosil <Darko.Prenosil@finteh.hr>
 * Shridhar Daithankar <shridhar_daithankar@persistent.co.in>
 *
 * contrib/dblink/dblink.c
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
#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "libpq/libpq-fe.h"
#include "funcapi.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/heap.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/heapam.h"
#include "commands/extension.h"
#include "dblink.h"
#include "storage/ipc.h"

PG_MODULE_MAGIC;

/*
 * Internal declarations
 */
static Datum dblink_record_internal(FunctionCallInfo fcinfo, bool is_async);
static void prepTuplestoreResult(FunctionCallInfo fcinfo);
static void materializeResult(FunctionCallInfo fcinfo, PGconn* conn, PGresult* res);
static void materializeQueryResult(
    FunctionCallInfo fcinfo, Linker* linker, const char* conname, const char* sql, bool fail);
static PGresult* storeQueryResult(storeInfo* sinfo, PGconn* conn, const char* sql);
static void storeRow(storeInfo* sinfo, PGresult* res, bool first);
static remoteConn* getConnectionByName(const char* name);
static HTAB* createConnHash(void);
static void createNewConnection(const char* name, remoteConn* rconn);
static void deleteConnection(const char* name);
static char** get_pkey_attnames(Relation rel, int16* indnkeyatts);
static char** get_text_array_contents(ArrayType* array, int* numitems);
static char* get_sql_insert(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals, char** tgt_pkattvals);
static char* get_sql_delete(Relation rel, int* pkattnums, int pknumatts, char** tgt_pkattvals);
static char* get_sql_update(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals, char** tgt_pkattvals);
static char* quote_ident_cstr(char* rawstr);
static int get_attnum_pk_pos(int* pkattnums, int pknumatts, int key);
static HeapTuple get_tuple_of_interest(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals);
static Relation get_rel_from_relname(text* relname_text, LOCKMODE lockmode, AclMode aclmode);
static char* generate_relation_name(Relation rel);
static void dblink_connstr_check(const char* connstr);
static void dblink_security_check(PGconn* conn);
static void dblink_res_error(const char* conname, PGresult* res, const char* dblink_context_msg, bool fail);
static char* get_connect_string(const char* servername);
static char* escape_param_str(const char* from);
static void validate_pkattnums(
    Relation rel, int2vector* pkattnums_arg, int32 pknumatts_arg, int** pkattnums, int* pknumatts);
static int applyRemoteGucs(PGconn* conn);
static void restoreLocalGucs(int nestlevel);
static uint32 dblink_index;
dblink_session_context* get_session_context();
static void storeRowInit(storeInfo* sinfo, int nfields, bool first);
static void GetDrivername(char* connstr_or_name, LinkInfo* linfo);

/* odbc */
static void ODBCstoreRow(storeInfo* sinfo, char** tupdata, SQLLEN* lenOut, SQLSMALLINT nfields, bool isFirst);
static bool UseODBCLinker(char* connstr);

#define PCONN (get_session_context()->pconn)
#define REMOTE_CONN_HASH (get_session_context()->remoteConnHash)
/* initial number of connection hashes */
#define NUMCONN 16
#define MAX_ERR_MSG_LEN 1000
#define MAX_BUF_LEN 100000
#define MAX_DRIVERNAME_LEN 50
#define DBLINK_NOTIFY_COLS 3

/* general utility */
#define xpfree(var_)        \
    do {                    \
        if (var_ != NULL) { \
            pfree(var_);    \
            var_ = NULL;    \
        }                   \
    } while (0)

#define xpstrdup(var_c, var_)      \
    do {                           \
        if (var_ != NULL)          \
            var_c = pstrdup(var_); \
        else                       \
            var_c = NULL;          \
    } while (0)

#define DBLINK_CONN_NOT_AVAIL                                                                                      \
    do {                                                                                                           \
        if (conname)                                                                                               \
            ereport(ERROR,                                                                                         \
                (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST), errmsg("connection \"%s\" not available", conname))); \
        else                                                                                                       \
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST), errmsg("connection not available")));      \
    } while (0)

#define DBLINK_GET_CONN                                                                 \
    do {                                                                                \
        char* conname_or_str = text_to_cstring(PG_GETARG_TEXT_PP(0));                   \
        rconn = getConnectionByName(conname_or_str);                                    \
        if (rconn) {                                                                    \
            linker = rconn->linker;                                                     \
            conname = conname_or_str;                                                   \
        } else {                                                                        \
            if(UseODBCLinker(conname_or_str)){                                          \
                ODBCLinker* olinker = New(SESS_GET_MEM_CXT_GROUP                        \
                    (MEMORY_CONTEXT_COMMUNICATION)) ODBCLinker(conname_or_str);         \
                linker = olinker;                                                       \
            } else {                                                                    \
                connstr = get_connect_string(conname_or_str);                           \
                if (connstr == NULL) {                                                  \
                    connstr = conname_or_str;                                           \
                }                                                                       \
                dblink_connstr_check(connstr);                                          \
                PQLinker* plinker = New(SESS_GET_MEM_CXT_GROUP                          \
                    (MEMORY_CONTEXT_COMMUNICATION)) PQLinker(connstr);                  \
                linker = plinker;                                                       \
            }                                                                           \
            freeconn = true;                                                            \
        }                                                                               \
    } while (0)

#define DBLINK_GET_NAMED_CONN                            \
    do {                                                 \
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0)); \
        rconn = getConnectionByName(conname);            \
        if (rconn) {                                     \
            linker = rconn->linker;                      \
        } else {                                         \
            DBLINK_CONN_NOT_AVAIL;                       \
        }                                                \
    } while (0)

#define DBLINK_INIT                                                                                   \
    do {                                                                                              \
        if (!PCONN) {                                                                                 \
            PCONN = (remoteConn*)MemoryContextAlloc(                                                  \
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), sizeof(remoteConn));            \
            PCONN->linker = NULL;                                                                     \
        }                                                                                             \
    } while (0)

static void DblinkQuitAndClean(int code, Datum arg)
{
    if (PCONN->linker != NULL) {
        PCONN->linker->finish();
        PCONN->linker = NULL;
    }
    
    HASH_SEQ_STATUS status;
    remoteConnHashEnt* hentry = NULL;

    if (REMOTE_CONN_HASH) {
        hash_seq_init(&status, REMOTE_CONN_HASH);
        while ((hentry = (remoteConnHashEnt*)hash_seq_search(&status)) != NULL) {
            hentry->rconn->linker->finish();
        }
        hash_destroy(REMOTE_CONN_HASH);
        REMOTE_CONN_HASH = NULL;
    }
}

void set_extension_index(uint32 index)
{
    dblink_index = index;
}

void init_session_vars(void)
{
    RepallocSessionVarsArrayIfNecessary();

    dblink_session_context* psc =
        (dblink_session_context*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(dblink_session_context));
    u_sess->attr.attr_common.extension_session_vars_array[dblink_index] = psc;

    psc->pconn = NULL;
    psc->remoteConnHash = NULL;
    psc->needFree = TRUE;
}

dblink_session_context* get_session_context()
{
    if (u_sess->attr.attr_common.extension_session_vars_array[dblink_index] == NULL) {
        init_session_vars();
    }
    return (dblink_session_context*)u_sess->attr.attr_common.extension_session_vars_array[dblink_index];
}

Linker::Linker()
{
    if (ENABLE_THREAD_POOL) {
        ereport(ERROR, 
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("dblink not support in thread pool")));
    }
}

PQLinker::PQLinker(char* connstr)
{
    this->conn = NULL;
    this->res = NULL;
    this->openCursorCount = 0;
    this->newXactForCursor = false;

    this->conn = PQconnectdb(connstr);
    char* msg;

    if (PQstatus(this->conn) == CONNECTION_BAD) {
        msg = pstrdup(PQerrorMessage(this->conn));
        PQfinish(this->conn);

        ereport(ERROR,
            (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                errmsg("could not establish connection"),
                    errdetail_internal("%s", msg)));
    }
    dblink_security_check(this->conn);
    /* attempt to set client encoding to match server encoding */
    PQsetClientEncoding(conn, GetDatabaseEncodingName());
}

void PQLinker::finish()
{
    PQfinish(this->conn);
}

text* PQLinker::exec(char* conname, const char* sql, bool fail)
{   
    text* volatile sql_cmd_status = NULL;

    this->res = PQexec(conn, sql);
    if (!res || (PQresultStatus(this->res) != PGRES_COMMAND_OK && PQresultStatus(this->res) != PGRES_TUPLES_OK)) {
        dblink_res_error(conname, this->res, "could not execute command", fail);
        /*
        * and save a copy of the command status string to return as our
        * result tuple
        */
        sql_cmd_status = cstring_to_text("ERROR");
    } else if (PQresultStatus(this->res) == PGRES_COMMAND_OK) {
        /*
        * and save a copy of the command status string to return as our
        * result tuple
        */
        sql_cmd_status = cstring_to_text(PQcmdStatus(this->res));
        PQclear(this->res);
    } else {
        PQclear(this->res);
        ereport(ERROR,
            (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                errmsg("statement returning results not allowed")));
    }
    return sql_cmd_status; 
}

char* PQLinker::errorMsg()
{
    char* msg = PQerrorMessage(this->conn);
    return msg;
}

int PQLinker::isBusy()
{
    PQconsumeInput(this->conn);
    return PQisBusy(this->conn);
}

char* PQLinker::cancel(PGcancel* cancel)
{
    char* errbuf = NULL;
    int res = 0;
    cancel = PQgetCancel(this->conn);
    res = PQcancel(cancel, errbuf, 256);
    PQfreeCancel(cancel);

    if (res == 1) {
        errbuf = "OK";
    }

    return errbuf;
}

int PQLinker::sendQuery(char* sql){
    int retval = 0;
    retval = PQsendQuery(this->conn, sql);
    if (retval != 1) {
        elog(NOTICE, "could not send query: %s", PQerrorMessage(conn));
    }      
    return 1;
}


char* PQLinker::open(char* conname, char* sql, bool fail)
{
    /* If we are not in a transaction, start one */
    if (PQtransactionStatus(this->conn) == PQTRANS_IDLE) {
        this->res = PQexec(this->conn, "START TRANSACTION");

        if (PQresultStatus(this->res) != PGRES_COMMAND_OK) {
            char* errmsg = pstrdup(PQerrorMessage(this->conn)); 
            if (res) {
                PQclear(res); 
            }                        
            elog(ERROR, "begin error: %s", errmsg); 
        }

        PQclear(this->res);
        this->newXactForCursor = TRUE;

        /*
         * Since transaction state was IDLE, we force cursor count to
         * initially be 0. This is needed as a previous ABORT might have wiped
         * out our transaction without maintaining the cursor count for us.
         */
        this->openCursorCount = 0;
    }

    /* if we started a transaction, increment cursor count */
    if (this->newXactForCursor)
        (this->openCursorCount)++;

    this->res = PQexec(this->conn, sql);
    if (!res || PQresultStatus(this->res) != PGRES_COMMAND_OK) {
        dblink_res_error(conname, this->res, "could not open cursor", fail);
        return "ERROR";
    }

    PQclear(this->res);
    return "OK";
}

char* PQLinker::close(char* conname, char* sql, bool fail)
{

    this->res = PQexec(conn, sql);
    if (!res || PQresultStatus(this->res) != PGRES_COMMAND_OK) {
        dblink_res_error(conname, this->res, "could not close cursor", fail);
        return "ERROR";
    }

    PQclear(this->res);

    /* if we started a transaction, decrement cursor count */
    if (this->newXactForCursor) {
        (this->openCursorCount)--;

        /* if count is zero, commit the transaction */
        if (this->openCursorCount == 0) {
            this->newXactForCursor = FALSE;

            this->res = PQexec(this->conn, "COMMIT");

            if (PQresultStatus(this->res) != PGRES_COMMAND_OK) {
                char* errmsg = pstrdup(PQerrorMessage(this->conn));
                if (this->res) {
                    PQclear(this->res);
                }
                elog(ERROR, "commit error: %s", errmsg);
            }
            PQclear(this->res);
        }
    }
    return "OK";
}

void PQLinker::getResult(char* conname, FunctionCallInfo fcinfo, char* sql, bool fail)
{
    /* async result retrieval, do it the old way */
    this->res = PQgetResult(this->conn);

    /* NULL means we're all done with the async results */
    if (this->res) {
        if (PQresultStatus(this->res) != PGRES_COMMAND_OK && PQresultStatus(this->res) != PGRES_TUPLES_OK) {
            dblink_res_error(conname, this->res, "could not execute query", fail);
            /* if fail isn't set, we'll return an empty query result */
        } else {
            materializeResult(fcinfo, this->conn, this->res);
        }
    }
}

void PQLinker::queryResult(ReturnSetInfo* rsinfo, const char* conname, storeInfo* sinfo, const char* sql, bool fail)
{
    PG_TRY();
    {
        /* execute query, collecting any tuples into the tuplestore */
        this->res = storeQueryResult(sinfo, this->conn, sql);
 
        if (!this->res || (PQresultStatus(this->res) != PGRES_COMMAND_OK && PQresultStatus(this->res) != PGRES_TUPLES_OK)) {
            /*
             * dblink_res_error will clear the passed PGresult, so we need
             * this ugly dance to avoid doing so twice during error exit
             */
            PGresult* res1 = this->res;

            this->res = NULL;
            dblink_res_error(conname, res1, "could not execute query", fail);
            /* if fail isn't set, we'll return an empty query result */
        } else if (PQresultStatus(this->res) == PGRES_COMMAND_OK) {
            /*
             * storeRow didn't get called, so we need to convert the command
             * status string to a tuple manually
             */
            TupleDesc tupdesc;
            AttInMetadata* attinmeta = NULL;
            Tuplestorestate* tupstore = NULL;
            HeapTuple tuple;
            char* values[1];
            MemoryContext oldcontext;

            /*
             * need a tuple descriptor representing one TEXT column to return
             * the command status string as our result tuple
             */
            tupdesc = CreateTemplateTupleDesc(1, false);
            TupleDescInitEntry(tupdesc, (AttrNumber)1, "status", TEXTOID, -1, 0);
            attinmeta = TupleDescGetAttInMetadata(tupdesc);

            oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
            tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
            rsinfo->setResult = tupstore;
            rsinfo->setDesc = tupdesc;
            MemoryContextSwitchTo(oldcontext);

            values[0] = PQcmdStatus(this->res);

            /* build the tuple and put it into the tuplestore. */
            tuple = BuildTupleFromCStrings(attinmeta, values);
            tuplestore_puttuple(tupstore, tuple);

            PQclear(this->res);
            this->res = NULL;
        } else {
            Assert(PQresultStatus(this->res) == PGRES_TUPLES_OK);
            /* storeRow should have created a tuplestore */
            Assert(rsinfo->setResult != NULL);

            PQclear(this->res);
            this->res = NULL;
        }
        PQclear(sinfo->last_res);
        sinfo->last_res = NULL;
        PQclear(sinfo->cur_res);
        sinfo->cur_res = NULL;
    }
    PG_CATCH();
    {
        /* be sure to release any libpq result we collected */
        PQclear(this->res);
        PQclear(sinfo->last_res);
        PQclear(sinfo->cur_res);
        /* and clear out any pending data in libpq */
        while ((this->res = PQgetResult(this->conn)) != NULL) {
            PQclear(this->res);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void PQLinker::fetch(char* conname, FunctionCallInfo fcinfo, const char* sql, bool fail, char* curname)
{
    /*
    * Try to execute the query.  Note that since libpq uses malloc, the
    * PGresult will be long-lived even though we are still in a short-lived
    * memory context.
    */
    this->res = PQexec(this->conn, sql);
    if (!this->res || (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK)) {
        dblink_res_error(conname, this->res, "could not fetch from cursor", fail);
        return;
    } else if (PQresultStatus(this->res) == PGRES_COMMAND_OK) {
        /* cursor does not exist - closed already or bad name */
        PQclear(this->res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_NAME), errmsg("cursor \"%s\" does not exist", curname)));
    }

    materializeResult(fcinfo, this->conn, this->res);
}

void PQLinker::getNotify(ReturnSetInfo* rsinfo)
{
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    PGnotify* notify = NULL;
    
    /* create the tuplestore in per-query memory */
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupdesc = CreateTemplateTupleDesc(DBLINK_NOTIFY_COLS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "notify_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "be_pid", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "extra", TEXTOID, -1, 0);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    PQconsumeInput(this->conn);
    while ((notify = PQnotifies(this->conn)) != NULL) {
        Datum values[DBLINK_NOTIFY_COLS] = {0};
        bool nulls[DBLINK_NOTIFY_COLS] = {false};

        if (notify->relname != NULL) {
            values[0] = CStringGetTextDatum(notify->relname);
        } else {
            values[1] = Int32GetDatum(notify->be_pid);
        }

            nulls[0] = true;

        if (notify->extra != NULL) {
            values[2] = CStringGetTextDatum(notify->extra);
        } else {
            nulls[2] = true;
        }

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);

        PQfreemem(notify);
        PQconsumeInput(this->conn);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);
}

ODBCLinker::ODBCLinker(char* connstr_or_name)
{
    this->msg = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP
            (MEMORY_CONTEXT_COMMUNICATION), sizeof(char*) * MAX_ERR_MSG_LEN);
    errno_t rc = strcpy_s(this->msg, MAX_ERR_MSG_LEN, "no error message");
    securec_check(rc, "\0", "\0");
    SQLINTEGER error = 0;

    error = SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE, &this->envHandle);
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        ereport(ERROR, 
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Error AllocHandle for Environment")));
    }

    SQLSetEnvAttr(this->envHandle, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

    error = SQLAllocHandle(SQL_HANDLE_DBC, this->envHandle, &this->connHandle);
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        SQLFreeHandle(SQL_HANDLE_ENV, this->envHandle);
        ereport(ERROR,
            (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                errmsg("Error AllocHandle for Connect")));
    }
    
    LinkInfo linfo;
    linfo.drivername = NULL;
    linfo.password = NULL;
    linfo.username = NULL;
    int len = strlen(connstr_or_name);
    GetDrivername(connstr_or_name, &linfo);
    /* atuo commit is the default value */
    error = SQLConnect(this->connHandle, linfo.drivername, SQL_NTS,
        linfo.username, SQL_NTS,  linfo.password, SQL_NTS);
    rc = memset_s(connstr_or_name, len, 0, len);
    securec_check(rc, "\0", "\0");

    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        SQLCHAR sqlcode[MAX_ERR_MSG_LEN];
        SQLGetDiagField(SQL_HANDLE_DBC, this->connHandle, 1, SQL_DIAG_MESSAGE_TEXT, &sqlcode, MAX_ERR_MSG_LEN, NULL);
        SQLFreeHandle(SQL_HANDLE_DBC, this->connHandle);
        SQLFreeHandle(SQL_HANDLE_ENV, this->envHandle);
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("Error SQLConnect\n%s", sqlcode)));
    }
}

void ODBCLinker::finish()
{
    if (this->stmt != NULL) {
        SQLFreeHandle(SQL_HANDLE_STMT, this->connHandle);
    }

    if (this->connHandle != NULL) {
        SQLDisconnect(this->connHandle);
        SQLFreeHandle(SQL_HANDLE_DBC, this->connHandle);
    }
    
    if (this->envHandle != NULL) {
        SQLFreeHandle(SQL_HANDLE_ENV, this->envHandle);
    }

    pfree(this);
    return;
}

text* ODBCLinker::exec(char* conname, const char* sql, bool fail)
{
    SQLINTEGER error = 0;               /* ERROR CODE */
    SQLHSTMT stmt = SQL_NULL_HSTMT;

    error = SQLAllocHandle(SQL_HANDLE_STMT, this->connHandle, &stmt);  
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Error AllocHandle for Statement")));
    }

    SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)0, 0);

    error = SQLExecDirect(stmt, (SQLCHAR*)sql, SQL_NTS);
    if (this->stmt != NULL) {
        SQLFreeHandle(SQL_HANDLE_STMT, this->stmt);
    } 
    this->stmt = stmt;
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        SQLError(this->envHandle, this->connHandle, this->stmt, NULL, NULL, (SQLCHAR*)this->msg, MAX_ERR_MSG_LEN, NULL);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Error exec\n%s", this->msg)));
    }
    return cstring_to_text("OK");
}

char* ODBCLinker::errorMsg()
{
    return this->msg;
}

int ODBCLinker::isBusy()
{
    SQLINTEGER ret = 0;
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;
    ret = SQLGetDiagField(SQL_HANDLE_STMT, this->stmt, 0, SQL_ATTR_ASYNC_ENABLE, outstr, 1024, &outstrlen);
    if (outstr[0] == 'y') {
        return 1;
    } else {
        return 0;
    }
}

char* ODBCLinker::cancel(PGcancel* cancel)
{
    SQLFreeStmt(this->stmt, SQL_CLOSE);
    return "OK";
}

int ODBCLinker::sendQuery(char *sql)
{
    SQLINTEGER error = 0;               /* ERROR CODE */
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    
    /* Specify that the statement is to be executed asynchronously. */
    error = SQLAllocHandle(SQL_HANDLE_STMT, this->connHandle, &stmt);
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Error AllocHandle for Statement")));
    }

    SQLSetStmtAttr(stmt, SQL_ATTR_ASYNC_ENABLE, (void*)SQL_ASYNC_ENABLE_ON, 0);  

    // When the statement has finished executing, retrieve the results.  
    error = SQLExecDirect(stmt, (SQLCHAR*)sql, SQL_NTS);
    if (this->stmt != NULL) {
        SQLFreeHandle(SQL_HANDLE_STMT, this->stmt);
    } 
    this->stmt = stmt;
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
        SQLError(this->envHandle, this->connHandle, this->stmt, NULL, NULL, (SQLCHAR*)this->msg, MAX_ERR_MSG_LEN, NULL);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Error exec\n%s", this->msg)));
    }
    return 1;
}

char* ODBCLinker::open(char* conname, char* sql, bool fail)
{
    this->exec(conname, sql, fail);
    return "OK";
}

char* ODBCLinker::close(char* conname, char* sql, bool fail)
{
    this->exec(conname, sql, fail);
    return "OK";
}

void ODBCLinker::getResult(char* conname, FunctionCallInfo fcinfo, char* sql, bool fail)
{
    prepTuplestoreResult(fcinfo);
    storeInfo sinfo;
    bool isFirst = true;
    SQLINTEGER error = 0;
    SQLSMALLINT nfields = 0;

    /* initialize storeInfo to empty */
    (void)memset_s(&sinfo, sizeof(sinfo), 0, sizeof(sinfo));
    sinfo.fcinfo = fcinfo;

    error = SQLNumResultCols(this->stmt, &nfields); 
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Error get colum number")));
    }

    SQLLEN lenOut[nfields];
    char** tupdata = (char**)palloc(sizeof(char*) * nfields);

    for (int i = 0; i < nfields; i++) {
        SQLColAttribute(this->stmt, i + 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &(lenOut[i]));
        tupdata[i] = (char*)palloc(sizeof(char) * (lenOut[i] + 1));
        SQLBindCol(this->stmt, i + 1, SQL_C_CHAR, (SQLPOINTER)tupdata[i], MAX_BUF_LEN, &(lenOut[i]));
    }

    while (SQLFetch(this->stmt) != SQL_NO_DATA) {
        CHECK_FOR_INTERRUPTS();
        ODBCstoreRow(&sinfo, tupdata, lenOut, nfields, isFirst);
        isFirst = false;
    }
    return;
}

void ODBCLinker::queryResult(ReturnSetInfo* rsinfo, const char* conname, storeInfo* sinfo, const char* sql, bool fail)
{
    bool isFirst = true;
    SQLINTEGER error = 0;
    SQLSMALLINT nfields = 0;

    this->exec(NULL, sql, true);

    /* get number of colum */
    error = SQLNumResultCols(this->stmt, &nfields); 
    if ((error != SQL_SUCCESS) && (error != SQL_SUCCESS_WITH_INFO)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Error get colum number")));
    }

    /* init data of tuple */
    SQLLEN lenOut[nfields];
    char** tupdata = (char**)palloc(sizeof(char*) * nfields);

    for (int i = 0; i < nfields; i++) {
        SQLColAttribute(stmt, i + 1, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, &(lenOut[i]));
        tupdata[i] = (char*)palloc(sizeof(char) * (lenOut[i] + 1));
        SQLBindCol(stmt, i + 1, SQL_C_CHAR, (SQLPOINTER)tupdata[i], MAX_BUF_LEN, &(lenOut[i]));
    }

    while (SQLFetch(stmt) != SQL_NO_DATA) {
        CHECK_FOR_INTERRUPTS();
        ODBCstoreRow(sinfo, tupdata, lenOut, nfields, isFirst);
        isFirst = false;
    }
    return;
}

void ODBCLinker::fetch(char* conname, FunctionCallInfo fcinfo, const char* sql, bool fail, char* curname)
{
    materializeQueryResult(fcinfo, this, NULL, sql, fail);
}

void ODBCLinker::getNotify(ReturnSetInfo* rsinfo)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("dblink_get_notify not support by odbc")));
}

/*
 * Create a persistent connection to another database
 */
PG_FUNCTION_INFO_V1(dblink_connect);
Datum dblink_connect(PG_FUNCTION_ARGS)
{
    char* conname_or_str = NULL;
    char* connstr = NULL;
    char* conname = NULL;
    remoteConn* rconn = NULL;
    
    if (ENABLE_THREAD_POOL) {
        ereport(ERROR, 
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("dblink not support in thread pool")));
    }

    DBLINK_INIT;

    if (get_session_context()->needFree) {
        on_proc_exit(DblinkQuitAndClean, 0);
        get_session_context()->needFree = FALSE;
    }

    if (PG_NARGS() == 2) {
        conname_or_str = text_to_cstring(PG_GETARG_TEXT_PP(1));
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
    } else if (PG_NARGS() == 1) {
        conname_or_str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    }
    
    if (conname) {
        rconn = (remoteConn*)MemoryContextAlloc(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), sizeof(remoteConn));
    }

    /* 
     * determine odbc or libpq 
     * if we have driver name , we choose odbc
     * otherwise we choose libpq
     */
    
    if (UseODBCLinker(conname_or_str)) {
        /* connect by odbc */
        ODBCLinker* olinker = New(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION)) ODBCLinker(conname_or_str);

        if (conname) {   
            rconn->linker = olinker;
            createNewConnection(conname, rconn);
        } else {
            if (PCONN->linker) {
                PCONN->linker->finish();
            }
            PCONN->linker = olinker;
        }
    } else {
        /* first check for valid foreign data server */
        connstr = get_connect_string(conname_or_str);
        if (connstr == NULL) {
            connstr = conname_or_str;
        }
        PQLinker* plinker = New(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION)) PQLinker(connstr);

        /* check password in connection string if not superuser */
        dblink_connstr_check(connstr);

        if (conname) {
            rconn->linker = plinker;
            createNewConnection(conname, rconn);
        } else {
            if (PCONN->linker) {
                PCONN->linker->finish();
            }
            PCONN->linker = plinker;      
        }
    }
    PG_RETURN_TEXT_P(cstring_to_text("OK"));
}

/*
 * Clear a persistent connection to another database
 */
PG_FUNCTION_INFO_V1(dblink_disconnect);
Datum dblink_disconnect(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    remoteConn* rconn = NULL;
    Linker* linker = NULL;

    DBLINK_INIT;

    /* first determine whether it is an unnamed link */
    if (PG_NARGS() == 1) {
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        rconn = getConnectionByName(conname);
        if (rconn) {
            linker = rconn->linker;
        }
    } else {
        linker = PCONN->linker;
    }

    if (linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }

    linker->finish();

    if (rconn) {
        deleteConnection(conname);
        pfree(rconn);
    } else {
        PCONN->linker = NULL;
    }

    PG_RETURN_TEXT_P(cstring_to_text("OK"));
}

/*
 * opens a cursor using a persistent connection
 */
PG_FUNCTION_INFO_V1(dblink_open);
Datum dblink_open(PG_FUNCTION_ARGS)
{
    char* msg = NULL;
    Linker* linker = NULL;
    char* curname = NULL;
    char* sql = NULL;
    char* conname = NULL;
    StringInfoData buf;
    remoteConn* rconn = NULL;
    bool fail = true; /* default to backward compatible behavior */

    DBLINK_INIT;
    initStringInfo(&buf);

    if (PG_NARGS() == 2) {
        /* text,text */
        curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
        rconn = PCONN;
    } else if (PG_NARGS() == 3) {
        /* might be text,text,text or text,text,bool */
        if (get_fn_expr_argtype(fcinfo->flinfo, 2) == BOOLOID) {
            curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
            fail = PG_GETARG_BOOL(2);
            rconn = PCONN;
        } else {
            conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
            sql = text_to_cstring(PG_GETARG_TEXT_PP(2));
            rconn = getConnectionByName(conname);
        }
    } else if (PG_NARGS() == 4) {
        /* text,text,text,bool */
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
        sql = text_to_cstring(PG_GETARG_TEXT_PP(2));
        fail = PG_GETARG_BOOL(3);
        rconn = getConnectionByName(conname);
    }

    if (rconn == NULL || rconn->linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }
    linker = rconn->linker;

    /* Assemble sql */
    appendStringInfo(&buf, "DECLARE %s CURSOR FOR %s", curname, sql);

    msg = linker->open(conname, buf.data, fail);

    PG_RETURN_TEXT_P(cstring_to_text(msg));
}

/*
 * closes a cursor
 */
PG_FUNCTION_INFO_V1(dblink_close);
Datum dblink_close(PG_FUNCTION_ARGS)
{
    Linker* linker = NULL;
    char* curname = NULL;
    char* conname = NULL;
    StringInfoData buf;
    char* msg = NULL;
    remoteConn* rconn = NULL;
    bool fail = true; /* default to backward compatible behavior */

    DBLINK_INIT;
    initStringInfo(&buf);

    if (PG_NARGS() == 1) {
        /* text */
        curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        rconn = PCONN;
    } else if (PG_NARGS() == 2) {
        /* might be text,text or text,bool */
        if (get_fn_expr_argtype(fcinfo->flinfo, 1) == BOOLOID) {
            curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            fail = PG_GETARG_BOOL(1);
            rconn = PCONN;
        } else {
            conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
            rconn = getConnectionByName(conname);
        }
    }
    if (PG_NARGS() == 3) {
        /* text,text,bool */
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
        fail = PG_GETARG_BOOL(2);
        rconn = getConnectionByName(conname);
    }

    if (rconn == NULL || rconn->linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }
    linker = rconn->linker;

    appendStringInfo(&buf, "CLOSE %s", curname);

    /* close the cursor */
    msg = linker->close(conname, buf.data, fail);

    PG_RETURN_TEXT_P(cstring_to_text(msg));
}

/*
 * Fetch results from an open cursor
 */
PG_FUNCTION_INFO_V1(dblink_fetch);
Datum dblink_fetch(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    remoteConn* rconn = NULL;
    Linker* linker = NULL;
    StringInfoData buf;
    char* curname = NULL;
    int howmany = 0;
    bool fail = true; /* default to backward compatible */

    prepTuplestoreResult(fcinfo);

    DBLINK_INIT;

    if (PG_NARGS() == 4) {
        /* text,text,int,bool */
        conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
        howmany = PG_GETARG_INT32(2);
        fail = PG_GETARG_BOOL(3);

        rconn = getConnectionByName(conname);
        if (rconn) {
            linker = rconn->linker;
        }
    } else if (PG_NARGS() == 3) {
        /* text,text,int or text,int,bool */
        if (get_fn_expr_argtype(fcinfo->flinfo, 2) == BOOLOID) {
            curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            howmany = PG_GETARG_INT32(1);
            fail = PG_GETARG_BOOL(2);
            linker = PCONN->linker;
        } else {
            conname = text_to_cstring(PG_GETARG_TEXT_PP(0));
            curname = text_to_cstring(PG_GETARG_TEXT_PP(1));
            howmany = PG_GETARG_INT32(2);

            rconn = getConnectionByName(conname);
            if (rconn) {
                linker = rconn->linker;
            }
        }
    } else if (PG_NARGS() == 2) {
        /* text,int */
        curname = text_to_cstring(PG_GETARG_TEXT_PP(0));
        howmany = PG_GETARG_INT32(1);
        linker = PCONN->linker;
    }

    if (linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }

    initStringInfo(&buf);
    appendStringInfo(&buf, "FETCH %d FROM %s", howmany, curname);
    
    linker->fetch(conname, fcinfo, buf.data, fail, curname);

    return (Datum)0;
}

/*
 * Note: this is the new preferred version of dblink
 */
PG_FUNCTION_INFO_V1(dblink_record);
Datum dblink_record(PG_FUNCTION_ARGS)
{
    return dblink_record_internal(fcinfo, false);
}

PG_FUNCTION_INFO_V1(dblink_send_query);
Datum dblink_send_query(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    Linker* linker = NULL;
    char* sql = NULL;
    remoteConn* rconn = NULL;
    int retval;

    if (PG_NARGS() == 2) {
        DBLINK_GET_NAMED_CONN;
        sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
    } else
        /* shouldn't happen */
        elog(ERROR, "wrong number of arguments");

    /* async query send */
    retval = linker->sendQuery(sql);

    PG_RETURN_INT32(retval);
}

PG_FUNCTION_INFO_V1(dblink_get_result);
Datum dblink_get_result(PG_FUNCTION_ARGS)
{
    return dblink_record_internal(fcinfo, true);
}

static Datum dblink_record_internal(FunctionCallInfo fcinfo, bool is_async)
{
    Linker* linker = NULL;
    volatile bool freeconn = false;

    prepTuplestoreResult(fcinfo);

    DBLINK_INIT;

    PG_TRY();
    {
        char* connstr = NULL;
        char* sql = NULL;
        char* conname = NULL;
        remoteConn* rconn = NULL;
        bool fail = true; /* default to backward compatible */
        bool is_three = !is_async && PG_NARGS() == 3;
        bool is_two = !is_async && PG_NARGS() == 2;
        bool is_one = !is_async && PG_NARGS() == 1;

        if (is_three) {
            /* text,text,bool */
            DBLINK_GET_CONN;
            sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
            fail = PG_GETARG_BOOL(2);
        } else if (is_two) {
            /* text,text or text,bool */
            if (get_fn_expr_argtype(fcinfo->flinfo, 1) == BOOLOID) {
                linker = PCONN->linker;
                sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
                fail = PG_GETARG_BOOL(1);
            } else {
                DBLINK_GET_CONN;
                sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
            }
        } else if (is_one) {
            /* text */
            linker = PCONN->linker;
            sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
        } else if (!is_async) {
            /* shouldn't happen */
            elog(ERROR, "wrong number of arguments");
        } /* is_async */
        /* get async result */
        else if (PG_NARGS() == 2) {
            /* text,bool */
            DBLINK_GET_NAMED_CONN;
            fail = PG_GETARG_BOOL(1);
        } else if (PG_NARGS() == 1) {
            /* text */
            DBLINK_GET_NAMED_CONN;
        } else {
            /* shouldn't happen */
            elog(ERROR, "wrong number of arguments");
        }

        if (linker == NULL) {
            DBLINK_CONN_NOT_AVAIL;
        }

        if (!is_async) {
            /* synchronous query, use efficient tuple collection method */
            materializeQueryResult(fcinfo, linker, conname, sql, fail);
        } else {
            linker->getResult(conname, fcinfo, sql, fail);
        }
    }
    PG_CATCH();
    {
        /* if needed, close the connection to the database */
        if (freeconn && linker) {
            linker->finish();
        }

        PG_RE_THROW();
    }
    PG_END_TRY();

    /* if needed, close the connection to the database */
    if (freeconn && linker) {
        linker->finish();
    }

    return (Datum)0;
}

/*
 * Verify function caller can handle a tuplestore result, and set up for that.
 *
 * Note: if the caller returns without actually creating a tuplestore, the
 * executor will treat the function result as an empty set.
 */
static void prepTuplestoreResult(FunctionCallInfo fcinfo)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    /* check to see if query supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));

    /* let the executor know we're sending back a tuplestore */
    rsinfo->returnMode = SFRM_Materialize;

    /* caller must fill these to return a non-empty result */
    rsinfo->setResult = NULL;
    rsinfo->setDesc = NULL;
}

/*
 * Copy the contents of the PGresult into a tuplestore to be returned
 * as the result of the current function.
 * The PGresult will be released in this function.
 */
static void materializeResult(FunctionCallInfo fcinfo, PGconn* conn, PGresult* res)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    /* prepTuplestoreResult must have been called previously */
    Assert(rsinfo->returnMode == SFRM_Materialize);

    PG_TRY();
    {
        TupleDesc tupdesc;
        bool is_sql_cmd = false;
        int ntuples;
        int nfields;

        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            is_sql_cmd = true;

            /*
             * need a tuple descriptor representing one TEXT column to return
             * the command status string as our result tuple
             */
            tupdesc = CreateTemplateTupleDesc(1, false);
            TupleDescInitEntry(tupdesc, (AttrNumber)1, "status", TEXTOID, -1, 0);
            ntuples = 1;
            nfields = 1;
        } else {
            Assert(PQresultStatus(res) == PGRES_TUPLES_OK);

            is_sql_cmd = false;

            /* get a tuple descriptor for our result type */
            switch (get_call_result_type(fcinfo, NULL, &tupdesc)) {
                case TYPEFUNC_COMPOSITE:
                    /* success */
                    break;
                case TYPEFUNC_RECORD:
                    /* failed to determine actual type of RECORD */
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("function returning record called in context "
                                   "that cannot accept type record")));
                    break;
                default:
                    /* result type isn't composite */
                    elog(ERROR, "return type must be a row type");
                    break;
            }

            /* make sure we have a persistent copy of the tupdesc */
            tupdesc = CreateTupleDescCopy(tupdesc);
            ntuples = PQntuples(res);
            nfields = PQnfields(res);
        }

        /*
         * check result and tuple descriptor have the same number of columns
         */
        if (nfields != tupdesc->natts)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("remote query result rowtype does not match "
                           "the specified FROM clause rowtype")));

        if (ntuples > 0) {
            AttInMetadata* attinmeta = NULL;
            int nestlevel = -1;
            Tuplestorestate* tupstore = NULL;
            MemoryContext oldcontext;
            int row;
            char** values;

            attinmeta = TupleDescGetAttInMetadata(tupdesc);

            /* Set GUCs to ensure we read GUC-sensitive data types correctly */
            if (!is_sql_cmd)
                nestlevel = applyRemoteGucs(conn);

            oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
            tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
            rsinfo->setResult = tupstore;
            rsinfo->setDesc = tupdesc;
            MemoryContextSwitchTo(oldcontext);

            values = (char**)palloc(nfields * sizeof(char*));

            /* put all tuples into the tuplestore */
            for (row = 0; row < ntuples; row++) {
                HeapTuple tuple;

                if (!is_sql_cmd) {
                    int i;

                    for (i = 0; i < nfields; i++) {
                        if (PQgetisnull(res, row, i))
                            values[i] = NULL;
                        else
                            values[i] = PQgetvalue(res, row, i);
                    }
                } else {
                    values[0] = PQcmdStatus(res);
                }

                /* build the tuple and put it into the tuplestore. */
                tuple = BuildTupleFromCStrings(attinmeta, values);
                tuplestore_puttuple(tupstore, tuple);
            }

            /* clean up GUC settings, if we changed any */
            restoreLocalGucs(nestlevel);

            /* clean up and return the tuplestore */
            tuplestore_donestoring(tupstore);
        }

        PQclear(res);
    }
    PG_CATCH();
    {
        /* be sure to release the libpq result */
        PQclear(res);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Execute the given SQL command and store its results into a tuplestore
 * to be returned as the result of the current function.
 *
 * This is equivalent to PQexec followed by materializeResult, but we make
 * use of libpq's single-row mode to avoid accumulating the whole result
 * inside libpq before it gets transferred to the tuplestore.
 */
static void materializeQueryResult(
    FunctionCallInfo fcinfo, Linker* linker, const char* conname, const char* sql, bool fail)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    storeInfo sinfo;

    /* prepTuplestoreResult must have been called previously */
    Assert(rsinfo->returnMode == SFRM_Materialize);

    /* initialize storeInfo to empty */
    (void)memset_s(&sinfo, sizeof(sinfo), 0, sizeof(sinfo));
    sinfo.fcinfo = fcinfo;

    linker->queryResult(rsinfo, conname, &sinfo, sql, fail);
}

/*
 * Execute query, and send any result rows to sinfo->tuplestore.
 */
static PGresult* storeQueryResult(storeInfo* sinfo, PGconn* conn, const char* sql)
{
    bool first = true;
    int nestlevel = -1;
    PGresult* res = NULL;

    if (!PQsendQuery(conn, sql))
        elog(ERROR, "could not send query: %s", PQerrorMessage(conn));

    if (!PQsetSingleRowMode(conn)) /* shouldn't fail */
        elog(ERROR, "failed to set single-row mode for dblink query");

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        sinfo->cur_res = PQgetResult(conn);
        if (!sinfo->cur_res)
            break;

        if (PQresultStatus(sinfo->cur_res) == PGRES_SINGLE_TUPLE) {
            /* got one row from possibly-bigger resultset */

            /*
             * Set GUCs to ensure we read GUC-sensitive data types correctly.
             * We shouldn't do this until we have a row in hand, to ensure
             * libpq has seen any earlier ParameterStatus protocol messages.
             */
            if (first && nestlevel < 0)
                nestlevel = applyRemoteGucs(conn);

            storeRow(sinfo, sinfo->cur_res, first);

            PQclear(sinfo->cur_res);
            sinfo->cur_res = NULL;
            first = false;
        } else {
            /* if empty resultset, fill tuplestore header */
            if (first && PQresultStatus(sinfo->cur_res) == PGRES_TUPLES_OK)
                storeRow(sinfo, sinfo->cur_res, first);

            /* store completed result at last_res */
            PQclear(sinfo->last_res);
            sinfo->last_res = sinfo->cur_res;
            sinfo->cur_res = NULL;
            first = true;
        }
    }

    /* clean up GUC settings, if we changed any */
    restoreLocalGucs(nestlevel);

    /* return last_res */
    res = sinfo->last_res;
    sinfo->last_res = NULL;
    return res;
}

/*
 * Send single row to sinfo->tuplestore.
 *
 * If "first" is true, create the tuplestore using PGresult's metadata
 * (in this case the PGresult might contain either zero or one row).
 */
static void storeRow(storeInfo* sinfo, PGresult* res, bool first)
{
    int nfields = PQnfields(res);
    HeapTuple tuple;
    int i;
    MemoryContext oldcontext;
    storeRowInit(sinfo, nfields, first);

    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(sinfo->tmpcontext);

    /* Done if empty resultset */
    if (PQntuples(res) == 0)
        return;

     /* Should have a single-row result if we get here */
    Assert(PQntuples(res) == 1);
    
    /*
     * Fill cstrs with null-terminated strings of column values.
     */
    for (i = 0; i < nfields; i++) {
        if (PQgetisnull(res, 0, i)) {
            sinfo->cstrs[i] = NULL;
        } else {
            sinfo->cstrs[i] = PQgetvalue(res, 0, i);
        }
    }

    /* Convert row to a tuple, and add it to the tuplestore */
    tuple = BuildTupleFromCStrings(sinfo->attinmeta, sinfo->cstrs);

    tuplestore_puttuple(sinfo->tuplestore, tuple);

    /* Clean up */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(sinfo->tmpcontext);
}

/*
 * List all open dblink connections by name.
 * Returns an array of all connection names.
 * Takes no params
 */
PG_FUNCTION_INFO_V1(dblink_get_connections);
Datum dblink_get_connections(PG_FUNCTION_ARGS)
{
    HASH_SEQ_STATUS status;
    remoteConnHashEnt* hentry = NULL;
    ArrayBuildState* astate = NULL;

    if (REMOTE_CONN_HASH) {
        hash_seq_init(&status, REMOTE_CONN_HASH);
        while ((hentry = (remoteConnHashEnt*)hash_seq_search(&status)) != NULL) {
            /* stash away current value */
            astate = accumArrayResult(astate, CStringGetTextDatum(hentry->name), false, TEXTOID, CurrentMemoryContext);
        }
    }

    if (astate)
        PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
    else
        PG_RETURN_NULL();
}

/*
 * Checks if a given remote connection is busy
 *
 * Returns 1 if the connection is busy, 0 otherwise
 * Params:
 *	text connection_name - name of the connection to check
 *
 */
PG_FUNCTION_INFO_V1(dblink_is_busy);
Datum dblink_is_busy(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    Linker* linker = NULL;
    remoteConn* rconn = NULL;

    DBLINK_INIT;
    DBLINK_GET_NAMED_CONN;

    PG_RETURN_INT32(linker->isBusy());
}

/*
 * Cancels a running request on a connection
 *
 * Returns text:
 *	"OK" if the cancel request has been sent correctly,
 *		an error message otherwise
 *
 * Params:
 *	text connection_name - name of the connection to check
 *
 */
PG_FUNCTION_INFO_V1(dblink_cancel_query);
Datum dblink_cancel_query(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    Linker* linker = NULL;
    remoteConn* rconn = NULL;
    PGcancel* cancel = NULL;
    char* errbuf;

    DBLINK_INIT;
    DBLINK_GET_NAMED_CONN;

    errbuf = linker->cancel(cancel);

    PG_RETURN_TEXT_P(cstring_to_text(errbuf));
}

/*
 * Get error message from a connection
 *
 * Returns text:
 *	"OK" if no error, an error message otherwise
 *
 * Params:
 *	text connection_name - name of the connection to check
 *
 */
PG_FUNCTION_INFO_V1(dblink_error_message);
Datum dblink_error_message(PG_FUNCTION_ARGS)
{
    char* msg = NULL;
    char* conname = NULL;
    Linker* linker = NULL;
    remoteConn* rconn = NULL;

    DBLINK_INIT;
    DBLINK_GET_NAMED_CONN;

    if (linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }

    msg = linker->errorMsg();

    if (msg == NULL || msg[0] == '\0')
        PG_RETURN_TEXT_P(cstring_to_text("OK"));
    else
        PG_RETURN_TEXT_P(cstring_to_text(msg));
}

/*
 * Execute an SQL non-SELECT command
 */
PG_FUNCTION_INFO_V1(dblink_exec);
Datum dblink_exec(PG_FUNCTION_ARGS)
{
    text* volatile sql_cmd_status = NULL;
    Linker* linker = NULL;
    volatile bool freeconn = false;

    DBLINK_INIT;

    PG_TRY();
    {
        char* connstr = NULL;
        char* sql = NULL;
        char* conname = NULL;
        remoteConn* rconn = NULL;
        bool fail = true; /* default to backward compatible behavior */

        if (PG_NARGS() == 3) {
            /* must be text,text,bool */
            DBLINK_GET_CONN;
            sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
            fail = PG_GETARG_BOOL(2);
        } else if (PG_NARGS() == 2) {
            /* might be text,text or text,bool */
            if (get_fn_expr_argtype(fcinfo->flinfo, 1) == BOOLOID) {
                linker = PCONN->linker;
                sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
                fail = PG_GETARG_BOOL(1);
            } else {
                DBLINK_GET_CONN;
                sql = text_to_cstring(PG_GETARG_TEXT_PP(1));
            }
        } else if (PG_NARGS() == 1) {
            /* must be single text argument */
            linker = PCONN->linker;
            sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
        } else {
            /* shouldn't happen */
            elog(ERROR, "wrong number of arguments");
        }

        if (linker == NULL) {
            DBLINK_CONN_NOT_AVAIL;
        }
        
        sql_cmd_status = linker->exec(conname, sql, fail);
    }
    PG_CATCH();
    {
        /* if needed, close the connection to the database */
        if (freeconn && linker) {
            linker->finish();
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* if needed, close the connection to the database */
    if (freeconn && linker) {
        linker->finish();
    }

    PG_RETURN_TEXT_P(sql_cmd_status);
}

/*
 * dblink_get_pkey
 *
 * Return list of primary key fields for the supplied relation,
 * or NULL if none exists.
 */
PG_FUNCTION_INFO_V1(dblink_get_pkey);
Datum dblink_get_pkey(PG_FUNCTION_ARGS)
{
    int16 indnkeyatts;
    char** results;
    FuncCallContext* funcctx = NULL;
    int32 call_cntr;
    int32 max_calls;
    AttInMetadata* attinmeta = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        Relation rel;
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* open target relation */
        rel = get_rel_from_relname(PG_GETARG_TEXT_P(0), AccessShareLock, ACL_SELECT);

        /* get the array of attnums */
        results = get_pkey_attnames(rel, &indnkeyatts);

        relation_close(rel, AccessShareLock);

        /*
         * need a tuple descriptor representing one INT and one TEXT column
         */
        tupdesc = CreateTemplateTupleDesc(2, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "position", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "colname", TEXTOID, -1, 0);

        /*
         * Generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx->attinmeta = attinmeta;

        if ((results != NULL) && (indnkeyatts > 0)) {
            funcctx->max_calls = indnkeyatts;

            /* got results, keep track of them */
            funcctx->user_fctx = results;
        } else {
            /* fast track when no results */
            MemoryContextSwitchTo(oldcontext);
            SRF_RETURN_DONE(funcctx);
        }

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * initialize per-call variables
     */
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;

    results = (char**)funcctx->user_fctx;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls) /* do when there is more left to send */
    {
        char** values;
        HeapTuple tuple;
        Datum result;
        const int valLen = 12;

        values = (char**)palloc(2 * sizeof(char*));
        values[0] = (char*)palloc(valLen); /* sign, 10 digits, '\0' */

        int rc = sprintf_s(values[0], valLen, "%d", call_cntr + 1);
        securec_check_ss(rc, "\0", "\0");

        values[1] = results[call_cntr];

        /* build the tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx);
    }
}

/*
 * dblink_build_sql_insert
 *
 * Used to generate an SQL insert statement
 * based on an existing tuple in a local relation.
 * This is useful for selectively replicating data
 * to another server via dblink.
 *
 * API:
 * <relname> - name of local table of interest
 * <pkattnums> - an int2vector of attnums which will be used
 * to identify the local tuple of interest
 * <pknumatts> - number of attnums in pkattnums
 * <src_pkattvals_arry> - text array of key values which will be used
 * to identify the local tuple of interest
 * <tgt_pkattvals_arry> - text array of key values which will be used
 * to build the string for execution remotely. These are substituted
 * for their counterparts in src_pkattvals_arry
 */
PG_FUNCTION_INFO_V1(dblink_build_sql_insert);
Datum dblink_build_sql_insert(PG_FUNCTION_ARGS)
{
    text* relname_text = PG_GETARG_TEXT_P(0);
    int2vector* pkattnums_arg = (int2vector*)PG_GETARG_POINTER(1);
    int32 pknumatts_arg = PG_GETARG_INT32(2);
    ArrayType* src_pkattvals_arry = PG_GETARG_ARRAYTYPE_P(3);
    ArrayType* tgt_pkattvals_arry = PG_GETARG_ARRAYTYPE_P(4);
    Relation rel;
    int* pkattnums = NULL;
    int pknumatts;
    char** src_pkattvals;
    char** tgt_pkattvals;
    int src_nitems;
    int tgt_nitems;
    char* sql = NULL;

    /*
     * Open target relation.
     */
    rel = get_rel_from_relname(relname_text, AccessShareLock, ACL_SELECT);

    /*
     * Process pkattnums argument.
     */
    validate_pkattnums(rel, pkattnums_arg, pknumatts_arg, &pkattnums, &pknumatts);

    /*
     * Source array is made up of key values that will be used to locate the
     * tuple of interest from the local system.
     */
    src_pkattvals = get_text_array_contents(src_pkattvals_arry, &src_nitems);

    /*
     * There should be one source array key value for each key attnum
     */
    if (src_nitems != pknumatts)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("source key array length must match number of key "
                       "attributes")));

    /*
     * Target array is made up of key values that will be used to build the
     * SQL string for use on the remote system.
     */
    tgt_pkattvals = get_text_array_contents(tgt_pkattvals_arry, &tgt_nitems);

    /*
     * There should be one target array key value for each key attnum
     */
    if (tgt_nitems != pknumatts)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("target key array length must match number of key "
                       "attributes")));

    /*
     * Prep work is finally done. Go get the SQL string.
     */
    sql = get_sql_insert(rel, pkattnums, pknumatts, src_pkattvals, tgt_pkattvals);

    /*
     * Now we can close the relation.
     */
    relation_close(rel, AccessShareLock);

    /*
     * And send it
     */
    PG_RETURN_TEXT_P(cstring_to_text(sql));
}

/*
 * dblink_build_sql_delete
 *
 * Used to generate an SQL delete statement.
 * This is useful for selectively replicating a
 * delete to another server via dblink.
 *
 * API:
 * <relname> - name of remote table of interest
 * <pkattnums> - an int2vector of attnums which will be used
 * to identify the remote tuple of interest
 * <pknumatts> - number of attnums in pkattnums
 * <tgt_pkattvals_arry> - text array of key values which will be used
 * to build the string for execution remotely.
 */
PG_FUNCTION_INFO_V1(dblink_build_sql_delete);
Datum dblink_build_sql_delete(PG_FUNCTION_ARGS)
{
    text* relname_text = PG_GETARG_TEXT_P(0);
    int2vector* pkattnums_arg = (int2vector*)PG_GETARG_POINTER(1);
    int32 pknumatts_arg = PG_GETARG_INT32(2);
    ArrayType* tgt_pkattvals_arry = PG_GETARG_ARRAYTYPE_P(3);
    Relation rel;
    int* pkattnums = NULL;
    int pknumatts;
    char** tgt_pkattvals;
    int tgt_nitems;
    char* sql = NULL;

    /*
     * Open target relation.
     */
    rel = get_rel_from_relname(relname_text, AccessShareLock, ACL_SELECT);

    /*
     * Process pkattnums argument.
     */
    validate_pkattnums(rel, pkattnums_arg, pknumatts_arg, &pkattnums, &pknumatts);

    /*
     * Target array is made up of key values that will be used to build the
     * SQL string for use on the remote system.
     */
    tgt_pkattvals = get_text_array_contents(tgt_pkattvals_arry, &tgt_nitems);

    /*
     * There should be one target array key value for each key attnum
     */
    if (tgt_nitems != pknumatts)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("target key array length must match number of key "
                       "attributes")));

    /*
     * Prep work is finally done. Go get the SQL string.
     */
    sql = get_sql_delete(rel, pkattnums, pknumatts, tgt_pkattvals);

    /*
     * Now we can close the relation.
     */
    relation_close(rel, AccessShareLock);

    /*
     * And send it
     */
    PG_RETURN_TEXT_P(cstring_to_text(sql));
}

/*
 * dblink_build_sql_update
 *
 * Used to generate an SQL update statement
 * based on an existing tuple in a local relation.
 * This is useful for selectively replicating data
 * to another server via dblink.
 *
 * API:
 * <relname> - name of local table of interest
 * <pkattnums> - an int2vector of attnums which will be used
 * to identify the local tuple of interest
 * <pknumatts> - number of attnums in pkattnums
 * <src_pkattvals_arry> - text array of key values which will be used
 * to identify the local tuple of interest
 * <tgt_pkattvals_arry> - text array of key values which will be used
 * to build the string for execution remotely. These are substituted
 * for their counterparts in src_pkattvals_arry
 */
PG_FUNCTION_INFO_V1(dblink_build_sql_update);
Datum dblink_build_sql_update(PG_FUNCTION_ARGS)
{
    text* relname_text = PG_GETARG_TEXT_P(0);
    int2vector* pkattnums_arg = (int2vector*)PG_GETARG_POINTER(1);
    int32 pknumatts_arg = PG_GETARG_INT32(2);
    ArrayType* src_pkattvals_arry = PG_GETARG_ARRAYTYPE_P(3);
    ArrayType* tgt_pkattvals_arry = PG_GETARG_ARRAYTYPE_P(4);
    Relation rel;
    int* pkattnums = NULL;
    int pknumatts;
    char** src_pkattvals;
    char** tgt_pkattvals;
    int src_nitems;
    int tgt_nitems;
    char* sql = NULL;

    /*
     * Open target relation.
     */
    rel = get_rel_from_relname(relname_text, AccessShareLock, ACL_SELECT);

    /*
     * Process pkattnums argument.
     */
    validate_pkattnums(rel, pkattnums_arg, pknumatts_arg, &pkattnums, &pknumatts);

    /*
     * Source array is made up of key values that will be used to locate the
     * tuple of interest from the local system.
     */
    src_pkattvals = get_text_array_contents(src_pkattvals_arry, &src_nitems);

    /*
     * There should be one source array key value for each key attnum
     */
    if (src_nitems != pknumatts)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("source key array length must match number of key "
                       "attributes")));

    /*
     * Target array is made up of key values that will be used to build the
     * SQL string for use on the remote system.
     */
    tgt_pkattvals = get_text_array_contents(tgt_pkattvals_arry, &tgt_nitems);

    /*
     * There should be one target array key value for each key attnum
     */
    if (tgt_nitems != pknumatts)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmsg("target key array length must match number of key "
                       "attributes")));

    /*
     * Prep work is finally done. Go get the SQL string.
     */
    sql = get_sql_update(rel, pkattnums, pknumatts, src_pkattvals, tgt_pkattvals);

    /*
     * Now we can close the relation.
     */
    relation_close(rel, AccessShareLock);

    /*
     * And send it
     */
    PG_RETURN_TEXT_P(cstring_to_text(sql));
}

/*
 * dblink_current_query
 * return the current query string
 * to allow its use in (among other things)
 * rewrite rules
 */
PG_FUNCTION_INFO_V1(dblink_current_query);
Datum dblink_current_query(PG_FUNCTION_ARGS)
{
    /* This is now just an alias for the built-in function current_query() */
    PG_RETURN_DATUM(current_query(fcinfo));
}

/*
 * Retrieve async notifications for a connection.
 *
 * Returns a setof record of notifications, or an empty set if none received.
 * Can optionally take a named connection as parameter, but uses the unnamed
 * connection per default.
 *
 */

PG_FUNCTION_INFO_V1(dblink_get_notify);
Datum dblink_get_notify(PG_FUNCTION_ARGS)
{
    char* conname = NULL;
    Linker* linker = NULL;
    remoteConn* rconn = NULL;
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    prepTuplestoreResult(fcinfo);

    DBLINK_INIT;
    if (PG_NARGS() == 1)
        DBLINK_GET_NAMED_CONN;
    else {
        linker = PCONN->linker;
    }

    if (linker == NULL) {
        DBLINK_CONN_NOT_AVAIL;
    }

    linker->getNotify(rsinfo);

    return (Datum)0;
}

PG_FUNCTION_INFO_V1(dblink_get_drivername);
Datum dblink_get_drivername(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR, 
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Only system admin can use this function."))));
    }
    
    char* odbcini = getenv("ODBCINI");
    StringInfoData res;
    initStringInfo(&res);
    char* buf = (char*)palloc(sizeof(char) * MAX_DRIVERNAME_LEN);
    /* The character of the cursor */
    char c;  
    /* Buffer string index */
    int i = 0;
    bool first = true;

    FILE* file = fopen(odbcini,"r");
    if (!file) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("can not open file")));
    }

    /* Traversing files,get drivername */
    while ((c = fgetc(file)) != EOF) {
        if (c == '[') {
            i = 0;
            buf[0] = '\0';
            c = fgetc(file);
            while (c != ']') {
                buf[i] = c;
                i++;
                if ((c = fgetc(file)) == EOF || i >= MAX_DRIVERNAME_LEN) {
                    break;
                }
            }
            buf[i] = '\0';

            if (first) {
                appendStringInfo(&res, "%s", buf);
                first = false;
            } else {
                appendStringInfo(&res, ",%s", buf);
            }
        }
    }
    fclose(file);
    PG_RETURN_TEXT_P(cstring_to_text(res.data));
}

/*************************************************************
 * internal functions
 */

/*
 * get_pkey_attnames
 *
 * Get the primary key attnames for the given relation.
 * Return NULL, and set indnkeyatts = 0, if no primary key exists.
 */
static char** get_pkey_attnames(Relation rel, int16* indnkeyatts)
{
    Relation indexRelation;
    ScanKeyData skey;
    SysScanDesc scan;
    HeapTuple indexTuple;
    int i;
    char** result = NULL;
    TupleDesc tupdesc;

    /* initialize indnkeyatts to 0 in case no primary key exists */
    *indnkeyatts = 0;

    tupdesc = rel->rd_att;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    indexRelation = heap_open(IndexRelationId, AccessShareLock);
    ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));

    scan = systable_beginscan(indexRelation, IndexIndrelidIndexId, true, NULL, 1, &skey);

    while (HeapTupleIsValid(indexTuple = systable_getnext(scan))) {
        Form_pg_index index = (Form_pg_index)GETSTRUCT(indexTuple);

        /* we're only interested if it is the primary key */
        if (index->indisprimary) {
            *indnkeyatts = GetIndexKeyAttsByTuple(NULL, indexTuple);
            if (*indnkeyatts > 0) {
                result = (char**)palloc(*indnkeyatts * sizeof(char*));

                for (i = 0; i < *indnkeyatts; i++) {
                    result[i] = SPI_fname(tupdesc, index->indkey.values[i]);
                }
            }
            break;
        }
    }

    systable_endscan(scan);
    heap_close(indexRelation, AccessShareLock);

    return result;
}

/*
 * Deconstruct a text[] into C-strings (note any NULL elements will be
 * returned as NULL pointers)
 */
static char** get_text_array_contents(ArrayType* array, int* numitems)
{
    int ndim = ARR_NDIM(array);
    int* dims = ARR_DIMS(array);
    int nitems;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    char** values;
    char* ptr = NULL;
    bits8* bitmap = NULL;
    int bitmask;
    int i;

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    *numitems = nitems = ArrayGetNItems(ndim, dims);

    get_typlenbyvalalign(ARR_ELEMTYPE(array), &typlen, &typbyval, &typalign);

    values = (char**)palloc(nitems * sizeof(char*));

    ptr = ARR_DATA_PTR(array);
    bitmap = ARR_NULLBITMAP(array);
    bitmask = 1;

    for (i = 0; i < nitems; i++) {
        if (bitmap && (*bitmap & bitmask) == 0) {
            values[i] = NULL;
        } else {
            values[i] = TextDatumGetCString(PointerGetDatum(ptr));
            ptr = att_addlength_pointer(ptr, typlen, ptr);
            ptr = (char*)att_align_nominal(ptr, typalign);
        }

        /* advance bitmap pointer if any */
        if (bitmap) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    return values;
}

static char* get_sql_insert(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals, char** tgt_pkattvals)
{
    char* relname = NULL;
    HeapTuple tuple;
    TupleDesc tupdesc;
    int natts;
    StringInfoData buf;
    char* val = NULL;
    int key;
    int i;
    bool needComma = false;

    initStringInfo(&buf);

    /* get relation name including any needed schema prefix and quoting */
    relname = generate_relation_name(rel);

    tupdesc = rel->rd_att;
    natts = tupdesc->natts;

    tuple = get_tuple_of_interest(rel, pkattnums, pknumatts, src_pkattvals);
    if (!tuple)
        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("source row not found")));

    appendStringInfo(&buf, "INSERT INTO %s(", relname);

    needComma = false;
    for (i = 0; i < natts; i++) {
        if (tupdesc->attrs[i].attisdropped)
            continue;

        if (needComma)
            appendStringInfo(&buf, ",");

        appendStringInfoString(&buf, quote_ident_cstr(NameStr(tupdesc->attrs[i].attname)));
        needComma = true;
    }

    appendStringInfo(&buf, ") VALUES(");

    /*
     * Note: i is physical column number (counting from 0).
     */
    needComma = false;
    for (i = 0; i < natts; i++) {
        if (tupdesc->attrs[i].attisdropped)
            continue;

        if (needComma)
            appendStringInfo(&buf, ",");

        key = get_attnum_pk_pos(pkattnums, pknumatts, i);

        if (key >= 0)
            val = tgt_pkattvals[key] ? pstrdup(tgt_pkattvals[key]) : NULL;
        else
            val = SPI_getvalue(tuple, tupdesc, i + 1);

        if (val != NULL) {
            appendStringInfoString(&buf, quote_literal_cstr(val));
            pfree(val);
        } else
            appendStringInfo(&buf, "NULL");
        needComma = true;
    }
    appendStringInfo(&buf, ")");

    return (buf.data);
}

static char* get_sql_delete(Relation rel, int* pkattnums, int pknumatts, char** tgt_pkattvals)
{
    char* relname = NULL;
    TupleDesc tupdesc;
    StringInfoData buf;
    int i;

    initStringInfo(&buf);

    /* get relation name including any needed schema prefix and quoting */
    relname = generate_relation_name(rel);

    tupdesc = rel->rd_att;

    appendStringInfo(&buf, "DELETE FROM %s WHERE ", relname);
    for (i = 0; i < pknumatts; i++) {
        int pkattnum = pkattnums[i];

        if (i > 0)
            appendStringInfo(&buf, " AND ");

        appendStringInfoString(&buf, quote_ident_cstr(NameStr(tupdesc->attrs[pkattnum].attname)));

        if (tgt_pkattvals[i] != NULL)
            appendStringInfo(&buf, " = %s", quote_literal_cstr(tgt_pkattvals[i]));
        else
            appendStringInfo(&buf, " IS NULL");
    }

    return (buf.data);
}

static char* get_sql_update(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals, char** tgt_pkattvals)
{
    char* relname = NULL;
    HeapTuple tuple;
    TupleDesc tupdesc;
    int natts;
    StringInfoData buf;
    char* val = NULL;
    int key;
    int i;
    bool needComma = false;

    initStringInfo(&buf);

    /* get relation name including any needed schema prefix and quoting */
    relname = generate_relation_name(rel);

    tupdesc = rel->rd_att;
    natts = tupdesc->natts;

    tuple = get_tuple_of_interest(rel, pkattnums, pknumatts, src_pkattvals);
    if (!tuple)
        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("source row not found")));

    appendStringInfo(&buf, "UPDATE %s SET ", relname);

    /*
     * Note: i is physical column number (counting from 0).
     */
    needComma = false;
    for (i = 0; i < natts; i++) {
        if (tupdesc->attrs[i].attisdropped)
            continue;

        if (needComma)
            appendStringInfo(&buf, ", ");

        appendStringInfo(&buf, "%s = ", quote_ident_cstr(NameStr(tupdesc->attrs[i].attname)));

        key = get_attnum_pk_pos(pkattnums, pknumatts, i);

        if (key >= 0)
            val = tgt_pkattvals[key] ? pstrdup(tgt_pkattvals[key]) : NULL;
        else
            val = SPI_getvalue(tuple, tupdesc, i + 1);

        if (val != NULL) {
            appendStringInfoString(&buf, quote_literal_cstr(val));
            pfree(val);
        } else
            appendStringInfoString(&buf, "NULL");
        needComma = true;
    }

    appendStringInfo(&buf, " WHERE ");

    for (i = 0; i < pknumatts; i++) {
        int pkattnum = pkattnums[i];

        if (i > 0)
            appendStringInfo(&buf, " AND ");

        appendStringInfo(&buf, "%s", quote_ident_cstr(NameStr(tupdesc->attrs[pkattnum].attname)));

        val = tgt_pkattvals[i];

        if (val != NULL)
            appendStringInfo(&buf, " = %s", quote_literal_cstr(val));
        else
            appendStringInfo(&buf, " IS NULL");
    }

    return (buf.data);
}

/*
 * Return a properly quoted identifier.
 * Uses quote_ident in quote.c
 */
static char* quote_ident_cstr(char* rawstr)
{
    text* rawstr_text = NULL;
    text* result_text = NULL;
    char* result = NULL;

    rawstr_text = cstring_to_text(rawstr);
    result_text = DatumGetTextP(DirectFunctionCall1(quote_ident, PointerGetDatum(rawstr_text)));
    result = text_to_cstring(result_text);

    return result;
}

static int get_attnum_pk_pos(int* pkattnums, int pknumatts, int key)
{
    int i;

    /*
     * Not likely a long list anyway, so just scan for the value
     */
    for (i = 0; i < pknumatts; i++)
        if (key == pkattnums[i])
            return i;

    return -1;
}

static HeapTuple get_tuple_of_interest(Relation rel, int* pkattnums, int pknumatts, char** src_pkattvals)
{
    char* relname = NULL;
    TupleDesc tupdesc;
    int natts;
    StringInfoData buf;
    int ret;
    HeapTuple tuple;
    int i;

    /*
     * Connect to SPI manager
     */
    if ((ret = SPI_connect()) < 0)
        /* internal error */
        elog(ERROR, "SPI connect failure - returned %d", ret);

    initStringInfo(&buf);

    /* get relation name including any needed schema prefix and quoting */
    relname = generate_relation_name(rel);

    tupdesc = rel->rd_att;
    natts = tupdesc->natts;

    /*
     * Build sql statement to look up tuple of interest, ie, the one matching
     * src_pkattvals.  We used to use "SELECT *" here, but it's simpler to
     * generate a result tuple that matches the table's physical structure,
     * with NULLs for any dropped columns.	Otherwise we have to deal with two
     * different tupdescs and everything's very confusing.
     */
    appendStringInfoString(&buf, "SELECT ");

    for (i = 0; i < natts; i++) {
        if (i > 0)
            appendStringInfoString(&buf, ", ");

        if (tupdesc->attrs[i].attisdropped)
            appendStringInfoString(&buf, "NULL");
        else
            appendStringInfoString(&buf, quote_ident_cstr(NameStr(tupdesc->attrs[i].attname)));
    }

    appendStringInfo(&buf, " FROM %s WHERE ", relname);

    for (i = 0; i < pknumatts; i++) {
        int pkattnum = pkattnums[i];

        if (i > 0)
            appendStringInfo(&buf, " AND ");

        appendStringInfoString(&buf, quote_ident_cstr(NameStr(tupdesc->attrs[pkattnum].attname)));

        if (src_pkattvals[i] != NULL)
            appendStringInfo(&buf, " = %s", quote_literal_cstr(src_pkattvals[i]));
        else
            appendStringInfo(&buf, " IS NULL");
    }

    /*
     * Retrieve the desired tuple
     */
    ret = SPI_exec(buf.data, 0);
    pfree(buf.data);

    /*
     * Only allow one qualifying tuple
     */
    if ((ret == SPI_OK_SELECT) && (SPI_processed > 1))
        ereport(
            ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg("source criteria matched more than one record")));

    else if (ret == SPI_OK_SELECT && SPI_processed == 1) {
        SPITupleTable* tuptable = SPI_tuptable;

        tuple = SPI_copytuple(tuptable->vals[0]);
        SPI_finish();

        return tuple;
    } else {
        /*
         * no qualifying tuples
         */
        SPI_finish();

        return NULL;
    }

    /*
     * never reached, but keep compiler quiet
     */
    return NULL;
}

/*
 * Open the relation named by relname_text, acquire specified type of lock,
 * verify we have specified permissions.
 * Caller must close rel when done with it.
 */
static Relation get_rel_from_relname(text* relname_text, LOCKMODE lockmode, AclMode aclmode)
{
    RangeVar* relvar = NULL;
    Relation rel;
    AclResult aclresult;

    relvar = makeRangeVarFromNameList(textToQualifiedNameList(relname_text));
    rel = heap_openrv(relvar, lockmode);

    aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), aclmode);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_CLASS, RelationGetRelationName(rel));

    return rel;
}

/*
 * generate_relation_name - copied from ruleutils.c
 *		Compute the name to display for a relation
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
static char* generate_relation_name(Relation rel)
{
    char* nspname = NULL;
    char* result = NULL;

    /* Qualify the name if not visible in search path */
    if (RelationIsVisible(RelationGetRelid(rel)))
        nspname = NULL;
    else
        nspname = get_namespace_name(rel->rd_rel->relnamespace);

    result = quote_qualified_identifier(nspname, RelationGetRelationName(rel));

    return result;
}

static remoteConn* getConnectionByName(const char* name)
{
    remoteConnHashEnt* hentry = NULL;
    char* key = NULL;

    if (!REMOTE_CONN_HASH)
        REMOTE_CONN_HASH = createConnHash();

    key = pstrdup(name);
    truncate_identifier(key, strlen(key), false);
    hentry = (remoteConnHashEnt*)hash_search(REMOTE_CONN_HASH, key, HASH_FIND, NULL);

    if (hentry)
        return (hentry->rconn);

    return (NULL);
}

static HTAB* createConnHash(void)
{
    HASHCTL ctl;

    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(remoteConnHashEnt);

    return hash_create("Remote Con hash", NUMCONN, &ctl, HASH_ELEM);
}

static void createNewConnection(const char* name, remoteConn* rconn)
{
    remoteConnHashEnt* hentry = NULL;
    bool found = false;
    char* key = NULL;

    if (!REMOTE_CONN_HASH)
        REMOTE_CONN_HASH = createConnHash();

    key = pstrdup(name);
    truncate_identifier(key, strlen(key), true);
    hentry = (remoteConnHashEnt*)hash_search(REMOTE_CONN_HASH, key, HASH_ENTER, &found);

    if (found) {
        rconn->linker->finish();
        pfree(rconn);

        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("duplicate connection name")));
    }

    hentry->rconn = rconn;
    strlcpy(hentry->name, name, sizeof(hentry->name));
}

static void deleteConnection(const char* name)
{
    remoteConnHashEnt* hentry = NULL;
    bool found = false;
    char* key = NULL;

    if (!REMOTE_CONN_HASH)
        REMOTE_CONN_HASH = createConnHash();

    key = pstrdup(name);
    truncate_identifier(key, strlen(key), false);
    hentry = (remoteConnHashEnt*)hash_search(REMOTE_CONN_HASH, key, HASH_REMOVE, &found);

    if (!hentry)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("undefined connection name")));
}

static void dblink_security_check(PGconn* conn)
{
    if (!superuser()) {
        if (!PQconnectionUsedPassword(conn)) {
            PQfinish(conn);

            ereport(ERROR,
                (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                    errmsg("password is required"),
                    errdetail("Non-system-admin cannot connect if the server does not request a password."),
                    errhint("Target server's authentication method must be changed.")));
        }
    }
}

/*
 * For non-superusers, insist that the connstr specify a password.	This
 * prevents a password from being picked up from .pgpass, a service file,
 * the environment, etc.  We don't want the postgres user's passwords
 * to be accessible to non-superusers.
 */
static void dblink_connstr_check(const char* connstr)
{
    if (!superuser()) {
        PQconninfoOption* options = NULL;
        PQconninfoOption* option = NULL;
        bool connstr_gives_password = false;

        options = PQconninfoParse(connstr, NULL);
        if (options) {
            for (option = options; option->keyword != NULL; option++) {
                if (strcmp(option->keyword, "password") == 0) {
                    if (option->val != NULL && option->val[0] != '\0') {
                        connstr_gives_password = true;
                        break;
                    }
                }
            }
            PQconninfoFree(options);
        }

        if (!connstr_gives_password)
            ereport(ERROR,
                (errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                    errmsg("password is required"),
                    errdetail("Non-system-admin must provide a password in the connection string.")));
    }
}

static void dblink_res_error(const char* conname, PGresult* res, const char* dblink_context_msg, bool fail)
{
    int level;
    char* pg_diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    char* pg_diag_message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    char* pg_diag_message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
    char* pg_diag_message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    char* pg_diag_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
    int sqlstate;
    char* message_primary = NULL;
    char* message_detail = NULL;
    char* message_hint = NULL;
    char* message_context = NULL;
    const char* dblink_context_conname = "unnamed";

    if (fail)
        level = ERROR;
    else
        level = NOTICE;

    if (pg_diag_sqlstate)
        sqlstate = MAKE_SQLSTATE(
            pg_diag_sqlstate[0], pg_diag_sqlstate[1], pg_diag_sqlstate[2], pg_diag_sqlstate[3], pg_diag_sqlstate[4]);
    else
        sqlstate = ERRCODE_CONNECTION_FAILURE;

    xpstrdup(message_primary, pg_diag_message_primary);
    xpstrdup(message_detail, pg_diag_message_detail);
    xpstrdup(message_hint, pg_diag_message_hint);
    xpstrdup(message_context, pg_diag_context);

    if (res)
        PQclear(res);

    if (conname)
        dblink_context_conname = conname;

    ereport(level,
        (errcode(sqlstate),
            message_primary ? errmsg_internal("%s", message_primary) : errmsg("unknown error"),
            message_detail ? errdetail_internal("%s", message_detail) : 0,
            message_hint ? errhint("%s", message_hint) : 0,
            message_context ? errcontext("%s", message_context) : 0,
            errcontext(
                "Error occurred on dblink connection named \"%s\": %s.", dblink_context_conname, dblink_context_msg)));
}

/*
 * Obtain connection string for a foreign server
 */
static char* get_connect_string(const char* servername)
{
    ForeignServer* foreign_server = NULL;
    UserMapping* user_mapping = NULL;
    ListCell* cell = NULL;
    StringInfo buf = makeStringInfo();
    ForeignDataWrapper* fdw = NULL;
    AclResult aclresult;
    char* srvname = NULL;

    /* first gather the server connstr options */
    srvname = pstrdup(servername);
    truncate_identifier(srvname, strlen(srvname), false);
    foreign_server = GetForeignServerByName(srvname, true);

    if (foreign_server) {
        Oid serverid = foreign_server->serverid;
        Oid fdwid = foreign_server->fdwid;
        Oid userid = GetUserId();

        user_mapping = GetUserMapping(userid, serverid);
        fdw = GetForeignDataWrapper(fdwid);

        /* Check permissions, user must have usage on the server. */
        aclresult = pg_foreign_server_aclcheck(serverid, userid, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, foreign_server->servername);

        foreach (cell, fdw->options) {
            DefElem* def = (DefElem*)lfirst(cell);

            appendStringInfo(buf, "%s='%s' ", def->defname, escape_param_str(strVal(def->arg)));
        }

        foreach (cell, foreign_server->options) {
            DefElem* def = (DefElem*)lfirst(cell);

            appendStringInfo(buf, "%s='%s' ", def->defname, escape_param_str(strVal(def->arg)));
        }

        foreach (cell, user_mapping->options) {

            DefElem* def = (DefElem*)lfirst(cell);

            appendStringInfo(buf, "%s='%s' ", def->defname, escape_param_str(strVal(def->arg)));
        }

        return buf->data;
    } else
        return NULL;
}

/*
 * Escaping libpq connect parameter strings.
 *
 * Replaces "'" with "\'" and "\" with "\\".
 */
static char* escape_param_str(const char* str)
{
    const char* cp = NULL;
    StringInfo buf = makeStringInfo();

    for (cp = str; *cp; cp++) {
        if (*cp == '\\' || *cp == '\'')
            appendStringInfoChar(buf, '\\');
        appendStringInfoChar(buf, *cp);
    }

    return buf->data;
}

/*
 * Validate the PK-attnums argument for dblink_build_sql_insert() and related
 * functions, and translate to the internal representation.
 *
 * The user supplies an int2vector of 1-based logical attnums, plus a count
 * argument (the need for the separate count argument is historical, but we
 * still check it).  We check that each attnum corresponds to a valid,
 * non-dropped attribute of the rel.  We do *not* prevent attnums from being
 * listed twice, though the actual use-case for such things is dubious.
 * Note that before Postgres 9.0, the user's attnums were interpreted as
 * physical not logical column numbers; this was changed for future-proofing.
 *
 * The internal representation is a palloc'd int array of 0-based physical
 * attnums.
 */
static void validate_pkattnums(
    Relation rel, int2vector* pkattnums_arg, int32 pknumatts_arg, int** pkattnums, int* pknumatts)
{
    TupleDesc tupdesc = rel->rd_att;
    int natts = tupdesc->natts;
    int i;

    /* Don't take more array elements than there are */
    pknumatts_arg = Min(pknumatts_arg, pkattnums_arg->dim1);

    /* Must have at least one pk attnum selected */
    if (pknumatts_arg <= 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("number of key attributes must be > 0")));

    /* Allocate output array */
    *pkattnums = (int*)palloc(pknumatts_arg * sizeof(int));
    *pknumatts = pknumatts_arg;

    /* Validate attnums and convert to internal form */
    for (i = 0; i < pknumatts_arg; i++) {
        int pkattnum = pkattnums_arg->values[i];
        int lnum;
        int j;

        /* Can throw error immediately if out of range */
        if (pkattnum <= 0 || pkattnum > natts)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid attribute number %d", pkattnum)));

        /* Identify which physical column has this logical number */
        lnum = 0;
        for (j = 0; j < natts; j++) {
            /* dropped columns don't count */
            if (tupdesc->attrs[j].attisdropped)
                continue;

            if (++lnum == pkattnum)
                break;
        }

        if (j < natts)
            (*pkattnums)[i] = j;
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid attribute number %d", pkattnum)));
    }
}

/*
 * Copy the remote session's values of GUCs that affect datatype I/O
 * and apply them locally in a new GUC nesting level.  Returns the new
 * nestlevel (which is needed by restoreLocalGucs to undo the settings),
 * or -1 if no new nestlevel was needed.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls restoreLocalGucs.  If an error is
 * thrown in between, guc.c will take care of undoing the settings.
 */
static int applyRemoteGucs(PGconn* conn)
{
    static const char* const GUCsAffectingIO[] = {"DateStyle", "IntervalStyle"};

    int nestlevel = -1;
    uint32 i;

    for (i = 0; i < lengthof(GUCsAffectingIO); i++) {
        const char* gucName = GUCsAffectingIO[i];
        const char* remoteVal = PQparameterStatus(conn, gucName);
        const char* localVal = NULL;

        /*
         * If the remote server is pre-8.4, it won't have u_sess->attr.attr_common.IntervalStyle, but
         * that's okay because its output format won't be ambiguous.  So just
         * skip the GUC if we don't get a value for it.  (We might eventually
         * need more complicated logic with remote-version checks here.)
         */
        if (remoteVal == NULL)
            continue;

        /*
         * Avoid GUC-setting overhead if the remote and local GUCs already
         * have the same value.
         */
        localVal = GetConfigOption(gucName, false, false);
        Assert(localVal != NULL);

        if (strcmp(remoteVal, localVal) == 0)
            continue;

        /* Create new GUC nest level if we didn't already */
        if (nestlevel < 0)
            nestlevel = NewGUCNestLevel();

        /* Apply the option (this will throw error on failure) */
        (void)set_config_option(gucName, remoteVal, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0);
    }

    return nestlevel;
}

/*
 * Restore local GUCs after they have been overlaid with remote settings.
 */
static void restoreLocalGucs(int nestlevel)
{
    /* Do nothing if no new nestlevel was created */
    if (nestlevel > 0)
        AtEOXact_GUC(true, nestlevel);
}

/*
* Link by odbc
*/

static void GetDrivername(char* connstr_or_name, LinkInfo* linfo)
{
    char* p;
    p = strtok(connstr_or_name, " ");
    while(p != NULL) {
        if(strstr(p, "drivername=")){
            linfo->drivername = (SQLCHAR*)(p + 11);
        } else if(strstr(p, "user=")) {
            linfo->username = (SQLCHAR*)(p + 5);
        } else if(strstr(p, "password=")) {
            linfo->password = (SQLCHAR*)(p + 9);
        }
        p = strtok(NULL, " ");
    }

    if (linfo->username == NULL || linfo->password == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Error connect string")));
    }
    return;   
}

static bool UseODBCLinker(char* connstr)
{
    if (strstr(connstr, "drivername=") == NULL) {
        return false;
    } else {
        return true;
    }
}

static void ODBCstoreRow(storeInfo* sinfo, char** tupdata, SQLLEN* lenOut, SQLSMALLINT nfields, bool isFirst)
{
    HeapTuple tuple;
    int i;
    MemoryContext oldcontext;
    storeRowInit(sinfo, nfields, isFirst);

    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(sinfo->tmpcontext);

    /*
     * Fill cstrs with null-terminated strings of column values.
     */
    for (i = 0; i < nfields; i++) {
        if (lenOut[i] == -1) {
            sinfo->cstrs[i] = NULL;
        } else {
            sinfo->cstrs[i] = tupdata[i];
        }
    }

    /* Convert row to a tuple, and add it to the tuplestore */
    tuple = BuildTupleFromCStrings(sinfo->attinmeta, sinfo->cstrs);

    tuplestore_puttuple(sinfo->tuplestore, tuple);

    /* Clean up */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(sinfo->tmpcontext);
}

static void storeRowInit(storeInfo* sinfo, int nfields, bool first)
{
    MemoryContext oldcontext;
    if (first) {
        /* Prepare for new result set */
        ReturnSetInfo* rsinfo = (ReturnSetInfo*)sinfo->fcinfo->resultinfo;
        TupleDesc tupdesc;
        /*
         * It's possible to get more than one result set if the query string
         * contained multiple SQL commands.  In that case, we follow PQexec's
         * traditional behavior of throwing away all but the last result.
         */
        if (sinfo->tuplestore) {
            tuplestore_end(sinfo->tuplestore);
        }
        sinfo->tuplestore = NULL;

        /* get a tuple descriptor for our result type */
        switch (get_call_result_type(sinfo->fcinfo, NULL, &tupdesc)) {
            case TYPEFUNC_COMPOSITE:
                /* success */
                break;
            case TYPEFUNC_RECORD:
                /* failed to determine actual type of RECORD */
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("function returning record called in context "
                               "that cannot accept type record")));
                break;
            default:
                /* result type isn't composite */
                elog(ERROR, "return type must be a row type");
                break;
        }

        /* make sure we have a persistent copy of the tupdesc */
        tupdesc = CreateTupleDescCopy(tupdesc);

        /* check result and tuple descriptor have the same number of columns */
        if (nfields != tupdesc->natts)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("remote query result rowtype does not match "
                           "the specified FROM clause rowtype")));

        /* Prepare attinmeta for later data conversions */
        sinfo->attinmeta = TupleDescGetAttInMetadata(tupdesc);

        /* Create a new, empty tuplestore */
        oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
        sinfo->tuplestore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
        rsinfo->setResult = sinfo->tuplestore;
        rsinfo->setDesc = tupdesc;
        MemoryContextSwitchTo(oldcontext);

        /*
         * Set up sufficiently-wide string pointers array; this won't change
         * in size so it's easy to preallocate.
         */
        if (sinfo->cstrs)
            pfree(sinfo->cstrs);
        sinfo->cstrs = (char**)palloc(nfields * sizeof(char*));

        /* Create short-lived memory context for data conversions */
        if (!sinfo->tmpcontext)
            sinfo->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
                "dblink temporary context",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);
    }
}
