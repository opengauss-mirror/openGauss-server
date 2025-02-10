/*
 * Called before any actual parsing is done
 */
core_yyscan_t
pgtsql_scanner_init(const char *str,
					core_yy_extra_type *yyext,
					const ScanKeywordList *keywordlist,
					const uint16 *keyword_tokens)
{
	Size		slen = strlen(str);
	yyscan_t	scanner;

	GetSessionContext()->dialect_sql = true;

	if (yylex_init(&scanner) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("yylex_init() failed: %m")));

	pgtsql_core_yyset_extra(yyext, scanner);

	yyext->keywordlist = keywordlist;
	yyext->keyword_tokens = keyword_tokens;
	yyext->in_slash_proc_body = false;
	yyext->paren_depth = 0;
	yyext->query_string_locationlist = NIL;
	yyext->is_createstmt = false;
	yyext->dolqstart = NULL;
	yyext->is_hint_str = false;
	yyext->parameter_list = NIL;
	yyext->include_ora_comment = false;
	yyext->func_param_begin = 0;
	yyext->func_param_end = 0;
	yyext->return_pos_end = 0;

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	yyext->scanbuf = (char *) palloc(slen + 2);
	yyext->scanbuflen = slen;
	memcpy(yyext->scanbuf, str, slen);
	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	yy_scan_buffer(yyext->scanbuf, slen + 2, scanner);

	/* initialize literal buffer to a reasonable but expansible size */
	yyext->literalalloc = 1024;
	yyext->literalbuf = (char *) palloc(yyext->literalalloc);
	yyext->literallen = 0;
	yyext->warnOnTruncateIdent = true;

    /* plpgsql keyword params */
    yyext->isPlpgsqlKeyWord = false;
    yyext->plKeywordValue = NULL;
	yyext->is_delimiter_name = false;
	yyext->is_last_colon = false;
	yyext->is_proc_end = false;

	// Added CALL for procedure and function
	getDynaParamSeq("init", true, true, NULL);

	return scanner;
}

/*
 * Called after parsing is done to clean up after scanner_init()
 */
void
pgtsql_scanner_finish(core_yyscan_t yyscanner)
{
	scanner_finish(yyscanner);
}

void	   *core_yyalloc(yy_size_t bytes, core_yyscan_t yyscanner);

void *
pgtsql_core_yyalloc(yy_size_t bytes, core_yyscan_t yyscanner)
{
	return core_yyalloc(bytes, yyscanner);
}

void	   *core_yyrealloc(void *ptr, yy_size_t bytes, core_yyscan_t yyscanner);

void *
pgtsql_core_yyrealloc(void *ptr, yy_size_t bytes, core_yyscan_t yyscanner)
{
	return core_yyrealloc(ptr, bytes, yyscanner);
}

void
			core_yyfree(void *ptr, core_yyscan_t yyscanner);

void
pgtsql_core_yyfree(void *ptr, core_yyscan_t yyscanner)
{
	core_yyfree(ptr, yyscanner);
}

#define core_yyset_extra pgtsql_core_yyset_extra
