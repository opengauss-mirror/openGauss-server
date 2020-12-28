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
 * odbc_bridge.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/extension/connector/odbc_bridge.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ODBC_BRIDGE_H
#define ODBC_BRIDGE_H

#include <sql.h>

/* handle definition for class connection and result */
typedef void* CONN_HANDLE;
typedef void* RESULT_HANDLE;

extern "C" CONN_HANDLE odbc_lib_connect(const char* dsn, const char* user, const char* pass);
extern "C" void odbc_lib_cleanup(CONN_HANDLE, RESULT_HANDLE);

extern "C" RESULT_HANDLE odbc_lib_query(CONN_HANDLE conn, const char* query);
extern "C" void odbc_lib_get_result(
    RESULT_HANDLE result, unsigned int* types, void** buf, bool* nulls, int cols, bool& isend);

/* the type definition of the ODBC API function pinter */
typedef SQLRETURN SQL_API (*SQLGetDiagRecAPI)(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
    SQLCHAR* Sqlstate, SQLINTEGER* NativeError, SQLCHAR* MessageText, SQLSMALLINT BufferLength,
    SQLSMALLINT* TextLength);

typedef SQLRETURN SQL_API (*SQLAllocHandleAPI)(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE* OutputHandle);
typedef SQLRETURN SQL_API (*SQLFreeHandleAPI)(SQLSMALLINT HandleType, SQLHANDLE Handle);

typedef SQLRETURN SQL_API (*SQLGetInfoAPI)(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
    SQLSMALLINT BufferLength, SQLSMALLINT* StringLength);
typedef SQLRETURN SQL_API (*SQLGetConnectAttrAPI)(SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER Value,
    SQLINTEGER BufferLength, SQLINTEGER* StringLength);
typedef SQLRETURN SQL_API (*SQLSetConnectAttrAPI)(
    SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength);
typedef SQLRETURN SQL_API (*SQLSetEnvAttrAPI)(
    SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength);
typedef SQLRETURN SQL_API (*SQLSetStmtAttrAPI)(
    SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength);

typedef SQLRETURN SQL_API (*SQLConnectAPI)(SQLHDBC ConnectionHandle, SQLCHAR* ServerName, SQLSMALLINT NameLength1,
    SQLCHAR* UserName, SQLSMALLINT NameLength2, SQLCHAR* Authentication, SQLSMALLINT NameLength3);
typedef SQLRETURN SQL_API (*SQLDisconnectAPI)(SQLHDBC ConnectionHandle);
typedef SQLRETURN SQL_API (*SQLCancelAPI)(SQLHSTMT StatementHandle);

typedef SQLRETURN SQL_API (*SQLExecDirectAPI)(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength);

typedef SQLRETURN SQL_API (*SQLFetchAPI)(SQLHSTMT StatementHandle);
typedef SQLRETURN SQL_API (*SQLGetDataAPI)(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
    SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN* StrLen_or_Ind);

typedef SQLRETURN SQL_API (*SQLNumResultColsAPI)(SQLHSTMT StatementHandle, SQLSMALLINT* ColumnCount);
typedef SQLRETURN SQL_API (*SQLDescribeColAPI)(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR* ColumnName,
    SQLSMALLINT BufferLength, SQLSMALLINT* NameLength, SQLSMALLINT* DataType, SQLULEN* ColumnSize,
    SQLSMALLINT* DecimalDigits, SQLSMALLINT* Nullable);
typedef SQLRETURN SQL_API (*SQLBindColAPI)(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
    SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN* StrLen_or_Ind);

extern __thread SQLGetDiagRecAPI pSQLGetDiagRec;
extern __thread SQLAllocHandleAPI pSQLAllocHandle;
extern __thread SQLFreeHandleAPI pSQLFreeHandle;
extern __thread SQLGetInfoAPI pSQLGetInfo;
extern __thread SQLGetConnectAttrAPI pSQLGetConnectAttr;
extern __thread SQLSetConnectAttrAPI pSQLSetConnectAttr;
extern __thread SQLSetEnvAttrAPI pSQLSetEnvAttr;
extern __thread SQLSetStmtAttrAPI pSQLSetStmtAttr;
extern __thread SQLConnectAPI pSQLConnect;
extern __thread SQLDisconnectAPI pSQLDisconnect;
extern __thread SQLCancelAPI pSQLCancel;
extern __thread SQLExecDirectAPI pSQLExecDirect;
extern __thread SQLFetchAPI pSQLFetch;
extern __thread SQLGetDataAPI pSQLGetData;
extern __thread SQLNumResultColsAPI pSQLNumResultCols;
extern __thread SQLDescribeColAPI pSQLDescribeCol;
extern __thread SQLBindColAPI pSQLBindCol;

#endif /* ODBC_BRIDGE_H */
