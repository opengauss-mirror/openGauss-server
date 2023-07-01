/*-------------------------------------------------------------------------
 *
 * pl_unreserved_kwlist.h
 *
 * The keyword lists are kept in their own source files for use by
 * automatic tools.  The exact representation of a keyword is determined
 * by the PG_KEYWORD macro, which is not defined in this file; it can
 * be defined by the caller for special purposes.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/pl/plpgsql/src/pl_unreserved_kwlist.h
 *
 *-------------------------------------------------------------------------
 */

/* There is deliberately not an #ifndef PL_UNRESERVED_KWLIST_H here. */

/*
 * List of (keyword-name, keyword-token-value) pairs.
 *
 * Be careful not to put the same word into pl_reserved_kwlist.h.  Also be
 * sure that pl_gram.y's unreserved_keyword production agrees with this list.
 *
 * Note: gen_keywordlist.pl requires the entries to appear in ASCII order.
 */

/* name, value */
PG_KEYWORD("absolute", K_ABSOLUTE) 
PG_KEYWORD("alias", K_ALIAS) 
PG_KEYWORD("alter", K_ALTER) 
PG_KEYWORD("array", K_ARRAY) 
PG_KEYWORD("as", K_AS) 
PG_KEYWORD("backward", K_BACKWARD) 
PG_KEYWORD("bulk", K_BULK) 
PG_KEYWORD("call", K_CALL)
PG_KEYWORD("catalog_name", K_CATALOG_NAME)
PG_KEYWORD("class_origin", K_CLASS_ORIGIN)
PG_KEYWORD("collect", K_COLLECT)
PG_KEYWORD("column_name", K_COLUMN_NAME)
PG_KEYWORD("commit", K_COMMIT) 
PG_KEYWORD("condition", K_CONDITION)
PG_KEYWORD("constant", K_CONSTANT)
PG_KEYWORD("constraint_catalog", K_CONSTRAINT_CATALOG)
PG_KEYWORD("constraint_name", K_CONSTRAINT_NAME)
PG_KEYWORD("constraint_schema", K_CONSTRAINT_SCHEMA)
PG_KEYWORD("continue", K_CONTINUE) 
PG_KEYWORD("current", K_CURRENT) 
PG_KEYWORD("cursor", K_CURSOR)
PG_KEYWORD("cursor_name", K_CURSOR_NAME)
PG_KEYWORD("debug", K_DEBUG) 
PG_KEYWORD("detail", K_DETAIL) 
PG_KEYWORD("distinct", K_DISTINCT)
PG_KEYWORD("do", K_DO) 
PG_KEYWORD("dump", K_DUMP) 
PG_KEYWORD("errcode", K_ERRCODE) 
PG_KEYWORD("error", K_ERROR) 
PG_KEYWORD("except", K_EXCEPT)
PG_KEYWORD("exceptions", K_EXCEPTIONS) 
PG_KEYWORD("false", K_FALSE) 
PG_KEYWORD("first", K_FIRST) 
PG_KEYWORD("forward", K_FORWARD) 
PG_KEYWORD("found", K_FOUND) 
PG_KEYWORD("function", K_FUNCTION)
PG_KEYWORD("handler", K_HANDLER) 
PG_KEYWORD("hint", K_HINT) 
PG_KEYWORD("immediate", K_IMMEDIATE) 
PG_KEYWORD("index", K_INDEX) 
PG_KEYWORD("info", K_INFO) 
PG_KEYWORD("instantiation", K_INSTANTIATION)
PG_KEYWORD("intersect", K_INTERSECT)
PG_KEYWORD("is", K_IS) 
PG_KEYWORD("iterate", K_ITERATE)
PG_KEYWORD("last", K_LAST)
PG_KEYWORD("leave", K_LEAVE)
PG_KEYWORD("log", K_LOG) 
PG_KEYWORD("merge", K_MERGE) 
PG_KEYWORD("message", K_MESSAGE) 
PG_KEYWORD("message_text", K_MESSAGE_TEXT)
PG_KEYWORD("multiset", K_MULTISET)
PG_KEYWORD("mysql_errno", K_MYSQL_ERRNO) 
PG_KEYWORD("next", K_NEXT) 
PG_KEYWORD("no", K_NO) 
PG_KEYWORD("notice", K_NOTICE) 
PG_KEYWORD("number", K_NUMBER) 
PG_KEYWORD("option", K_OPTION) 
PG_KEYWORD("package", K_PACKAGE)
PG_KEYWORD("perform", K_PERFORM)
PG_KEYWORD("pg_exception_context", K_PG_EXCEPTION_CONTEXT)
PG_KEYWORD("pg_exception_detail", K_PG_EXCEPTION_DETAIL) 
PG_KEYWORD("pg_exception_hint", K_PG_EXCEPTION_HINT) 
PG_KEYWORD("pragma", K_PRAGMA)
PG_KEYWORD("prior", K_PRIOR)
PG_KEYWORD("procedure", K_PROCEDURE)
PG_KEYWORD("query", K_QUERY) 
PG_KEYWORD("record", K_RECORD)
PG_KEYWORD("relative", K_RELATIVE) 
PG_KEYWORD("release", K_RELEASE)
PG_KEYWORD("repeat", K_REPEAT)
PG_KEYWORD("replace", K_REPLACE)
PG_KEYWORD("resignal", K_RESIGNAL)
PG_KEYWORD("result_oid", K_RESULT_OID) 
PG_KEYWORD("returned_sqlstate", K_RETURNED_SQLSTATE)
PG_KEYWORD("reverse", K_REVERSE) 
PG_KEYWORD("rollback", K_ROLLBACK) 
PG_KEYWORD("row_count", K_ROW_COUNT) 
PG_KEYWORD("rowtype", K_ROWTYPE)
PG_KEYWORD("save", K_SAVE)
PG_KEYWORD("savepoint", K_SAVEPOINT)
PG_KEYWORD("schema_name", K_SCHEMA_NAME)
PG_KEYWORD("scroll", K_SCROLL)
PG_KEYWORD("signal", K_SIGNAL)
PG_KEYWORD("slice", K_SLICE)
PG_KEYWORD("sqlexception", K_SQLEXCEPTION)
PG_KEYWORD("sqlstate", K_SQLSTATE) 
PG_KEYWORD("sqlwarning", K_SQLWARNING)
PG_KEYWORD("stacked", K_STACKED)
PG_KEYWORD("subclass_origin", K_SUBCLASS_ORIGIN)
PG_KEYWORD("sys_refcursor", K_SYS_REFCURSOR)
PG_KEYWORD("table", K_TABLE)
PG_KEYWORD("table_name", K_TABLE_NAME)
PG_KEYWORD("true", K_TRUE)
PG_KEYWORD("type", K_TYPE)
PG_KEYWORD("union", K_UNION)
PG_KEYWORD("until", K_UNTIL)
PG_KEYWORD("use_column", K_USE_COLUMN)
PG_KEYWORD("use_variable", K_USE_VARIABLE)
PG_KEYWORD("variable_conflict", K_VARIABLE_CONFLICT)
PG_KEYWORD("varray", K_VARRAY)
PG_KEYWORD("warning", K_WARNING)
PG_KEYWORD("with", K_WITH)

