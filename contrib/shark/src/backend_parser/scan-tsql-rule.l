%{
	BEGIN(tsql);
%}

{xnstart}		{
					/* National character.
					 * We will pass this along as a normal character string,
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