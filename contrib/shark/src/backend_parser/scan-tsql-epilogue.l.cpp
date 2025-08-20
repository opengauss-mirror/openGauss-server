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

/* Determine whether a variable name is a predefined T-SQL global variable */
bool IsTsqlAtatGlobalVar(const char* varname)
{
    size_t varname_len = strlen(varname);

    const size_t MIN_VARNAME_LEN = 6;
    const size_t MAX_VARNAME_LEN = 14;

    if ((varname_len < MIN_VARNAME_LEN) || (varname_len > MAX_VARNAME_LEN)) {
        return false;
    }

    // List of all T-SQL global "@@" variables:
    return ((pg_strcasecmp("@@FETCH_STATUS", varname) == 0) ||
            (pg_strcasecmp("@@ROWCOUNT", varname) == 0) ||
            (pg_strcasecmp("@@SPID", varname) == 0) ||
            (pg_strcasecmp("@@PROCID", varname) == 0));
}

static bool IsTsqlTranStmt(const char *haystack, int haystackLen)
{
    char *tempstr = static_cast<char *>(palloc0(haystackLen + 1));
    char *temp = tempstr;
    int line = 1; /* lineno of haystack which split by \0 */
    const int twoLines = 2;
    bool foundNonBlankChar = false; /* mark if we find a non blank char after begin */
    errno_t rc = EOK;

    /* we have to make a copy, since haystack is const char* */
    rc = memcpy_s(tempstr, haystackLen + 1, haystack, haystackLen);
    securec_check_ss(rc, "\0", "\0");

    /* find if the 2nd line is prefixed by a valid transaction token */
    while (temp < tempstr + haystackLen) {
        /* there may be '\0' in the string, and should be skipped */
        if (*temp == '\0') {
            temp++;
            line++;
            /* we only search the 2nd line */
            if (line > twoLines) {
                break;
            }
        } else if (isspace(*temp)) {
        /* skip the blank char */
            temp++;
        } else {
            /* we found a non blank char after begin, do further checking */
            if (line == twoLines) {
                foundNonBlankChar = true;
            }
            /* For a transaction statement, all possible tokens after BEGIN are here */
            if (line == twoLines && (pg_strncasecmp(temp, "transaction", strlen("transaction")) == 0 ||
                pg_strncasecmp(temp, "tran", strlen("tran")) == 0 ||
                pg_strncasecmp(temp, "work", strlen("work")) == 0 ||
                pg_strncasecmp(temp, "try", strlen("try")) == 0 ||
                pg_strncasecmp(temp, "isolation", strlen("isolation")) == 0 ||
                pg_strncasecmp(temp, "read", strlen("read")) == 0 ||
                pg_strncasecmp(temp, "deferrable", strlen("deferrable")) == 0 ||
                pg_strncasecmp(temp, "not", strlen("not")) == 0 ||
                pg_strncasecmp(temp, ";", strlen(";")) == 0)) {
                FREE_POINTER(tempstr);
                return true;
            }

            temp += strlen(temp);
        }
    }

    FREE_POINTER (tempstr);

    /*
     * if all the char after begin are blank
     *    it is a trans stmt
     * else
     *    it is a anaynomous block stmt
     */
    return foundNonBlankChar ? false : true;
}
