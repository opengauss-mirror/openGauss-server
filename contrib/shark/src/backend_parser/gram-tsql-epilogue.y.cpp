void
pgtsql_parser_init(base_yy_extra_type *yyext)
{
	parser_init(yyext);
}

static void
pgtsql_base_yyerror(YYLTYPE * yylloc, core_yyscan_t yyscanner, const char *msg)
{
	base_yyerror(yylloc, yyscanner, msg);
}

#include "scan-backend.inc"
#undef SCANINC
