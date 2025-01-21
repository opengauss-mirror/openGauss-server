#ifndef PGTSQL_SCANNER_H
#define PGTSQL_SCANNER_H

#include "parser/scanner.h"

extern const uint16 pgtsql_ScanKeywordTokens[];

extern int	pgtsql_core_yylex(core_YYSTYPE *lvalp, YYLTYPE * llocp, core_yyscan_t yyscanner);

core_yyscan_t
			pgtsql_scanner_init(const char *str,
								core_yy_extra_type *yyext,
								const ScanKeywordList *keywordlist,
								const uint16 *keyword_tokens);

void
			pgtsql_scanner_finish(core_yyscan_t yyscanner);

#endif							/* PGTSQL_SCANNER_H */
