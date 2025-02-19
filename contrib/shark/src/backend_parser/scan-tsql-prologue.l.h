/*
 * Constant data exported from this file.  This array maps from the
 * zero-based keyword numbers returned by ScanKeywordLookup to the
 * Bison token numbers needed by gram.y.  This is exported because
 * callers need to pass it to scanner_init, if they are using the
 * standard keyword list ScanKeywords.
 */
#define PG_KEYWORD(kwname, value, category, collabel) value,

const uint16 pgtsql_ScanKeywordTokens[] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD

/*
 *  The following macro will inject DIALECT_TSQL value
 *  as the first token in the string being parsed.
 *  We use this mechanism to choose different dialects
 *  within the parser.  See the corresponding code
 *  in scanner_init()
 */

#define YY_USER_INIT                            \
	if (g_instance.raw_parser_hook[DB_CMPT_D] && GetSessionContext()->dialect_sql) \
	{                                             \
		GetSessionContext()->dialect_sql = false; \
		*yylloc = 0;                              \
		return DIALECT_TSQL;                       \
	}

/* need to undef to prevent an infinite-loop calling
 * pgtsql_core_yylex(...) inside pgtsql_core_yylex(...)
 */
#undef PG_YYLEX

#undef YY_DECL
#define YY_DECL int pgtsql_core_yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner)

extern bool IsTsqlAtatGlobalVar(const char *varname);
