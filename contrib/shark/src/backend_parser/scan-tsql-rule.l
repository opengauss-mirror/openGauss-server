%{
	BEGIN(tsql);
%}

{xnstart}		{
					/* National character.
					 * We will pass this along as a normal character string,
					 * but preceded with an internally-generated "NCHAR".
					 * but preceded with an internally-generated "NCHAR".
					 */
					int kwnum;

					SET_YYLLOC();
					yyless(1);				/* eat only 'n' this time */

					/* Cast national character to nvarchar for shark */
					kwnum = ScanKeywordLookup("nvarchar",
											yyextra->keywordlist);

					if (kwnum >= 0)
					{
						yyextra->is_hint_str = false;
						yylval->keyword = GetScanKeyword(kwnum, yyextra->keywordlist);
						return yyextra->keyword_tokens[kwnum];
					}
					else
					{
						/* If NCHAR isn't a keyword, just return "n" */
						yylval->str = pstrdup("n");
						yyextra->ident_quoted = false;
						yyextra->is_hint_str = false;
						return IDENT;
					}
				}

{xbrstart}		{
					if (ENABLE_SBR_IDENTIFIER) {
						SET_YYLLOC();
						BEGIN(xbr);
						startlit();
					} else {
						SET_YYLLOC();
						return yytext[0];
					}
				}

<xbr>{xbrinside}	{
						addlit(yytext, yyleng, yyscanner);
					}

<xbr>{xbrstop}		{
						char *ident;

						BEGIN(INITIAL);
						if (yyextra->literallen == 0)
							yyerror("zero-length delimited identifier");
						ident = litbufdup(yyscanner);
						ident = downcase_truncate_identifier(ident, yyextra->literallen, true);
						yylval->str = ident;
						yyleng = yyextra->literallen + 2;
						yyextra->ident_quoted = false;
						yyextra->is_hint_str = false;

						return IDENT;
					}

{set_identifier}	{
						SET_YYLLOC();
						char *set_ident =
							downcase_truncate_identifier(yytext, yyleng, yyextra->warnOnTruncateIdent);
						if (IsTsqlAtatGlobalVar(set_ident)) {
							yylval->str = set_ident;
							yyextra->ident_quoted = false;
							return TSQL_ATAT_IDENT;
						} else {
							yyless(2);
							yylval->str = pstrdup(yytext);
							yyextra->is_hint_str = false;
							return Op;
						}
					}

{identifier}	{
					int kwnum;
                    char *ident;

					SET_YYLLOC();

					/* Is it a keyword? */
					kwnum = ScanKeywordLookup(yytext, yyextra->keywordlist);

					yyextra->is_hint_str = false;
					bool isPlpgsqlKeyword = yyextra->isPlpgsqlKeyWord;
					bool need_ignore = (!isPlpgsqlKeyword && kwnum >= 0) ?
						semtc_is_token_in_ignore_keyword_list(yyextra->keyword_tokens[kwnum], false) : false;
					if (kwnum >= 0 && !need_ignore)
					{
						yylval->keyword = GetScanKeyword(kwnum, yyextra->keywordlist);
                        uint16 token = yyextra->keyword_tokens[kwnum];

						/* Find the CREATE PROCEDURE syntax and set dolqstart. */
						if (token == CREATE)
						{
							yyextra->is_createstmt = true;
						}
						else if (token == TRIGGER && yyextra->is_createstmt)
						{
							/* Create trigger don't need set dolqstart */
							yyextra->is_createstmt = false;
						}
						else if ((token == (isPlpgsqlKeyword? yyextra->plKeywordValue->procedure : PROCEDURE) ||
                           token == (isPlpgsqlKeyword? yyextra->plKeywordValue->function : FUNCTION))
							&& (yyextra->is_createstmt))
						{
							/* Make yyextra->dolqstart not NULL means its in a proc with $$. */
							yyextra->dolqstart = "";
						}
						else if (token == (isPlpgsqlKeyword? yyextra->plKeywordValue->begin : BEGIN_P))
						{
                            if (!(u_sess->parser_cxt.isCreateFuncOrProc || u_sess->plsql_cxt.curr_compile_context != NULL)) {
                                /* cases that have to be a trans stmt and fall quickly */
                                if (yyg->yy_hold_char == ';' || /* found ';' after 'begin' */
                                    yyg->yy_hold_char == '\0')  /* found '\0' after 'begin' */
                                    return BEGIN_NON_ANOYBLOCK;
                                /* look for other transaction stmt */
                                if (IsTsqlTranStmt(yyextra->scanbuf, yyextra->scanbuflen))
                                    return BEGIN_NON_ANOYBLOCK;
                            }
						}
                        else if (token == (isPlpgsqlKeyword? yyextra->plKeywordValue->select : SELECT) ||
                                 token == (isPlpgsqlKeyword? yyextra->plKeywordValue->update : UPDATE) ||
                                 token == (isPlpgsqlKeyword? yyextra->plKeywordValue->insert : INSERT) ||
                                 token == (isPlpgsqlKeyword? yyextra->plKeywordValue->Delete : DELETE_P) ||
                                 token == MERGE)
						{
							yyextra->is_hint_str = true;
						}

                        set_is_delimiter_name(yytext,yyscanner);
						return token;
					}

					/*
					 * No.  Convert the identifier to lower case, and truncate
					 * if necessary.
					 */
					ident = downcase_truncate_identifier(yytext, yyleng, yyextra->warnOnTruncateIdent);
					/* Is it _charset? */
					if (ident[0] == '_' &&
						ENABLE_MULTI_CHARSET &&
						pg_valid_server_encoding(ident + 1) >= 0) {
						yylval->str = pstrdup(ident + 1);
						yyextra->is_hint_str = false;
						yyextra->ident_quoted = false;
						return UNDERSCORE_CHARSET;
					}
					yylval->str = ident;
					yyextra->ident_quoted = false;
					set_is_delimiter_name(yytext,yyscanner);
					return IDENT;
				}

<tsql>{tsql_hex}	{
						SET_YYLLOC();
						yylval->str = pstrdup(yytext);
						return TSQL_XCONST;
					}