/*
 *	The target production for the whole parse.
 */
stmtblock:	DIALECT_TSQL tsql_stmtmulti
			{
				pg_yyget_extra(yyscanner)->parsetree = $2;
			}
		;

AexprConst:
			TSQL_XCONST
				{
					$$ = makeTSQLHexStringConst($1, @1);
				}
			;

/* the thrashing around here is to discard "empty" statements... */
tsql_stmtmulti:	tsql_stmtmulti ';' tsql_stmt
				{
					if ($3 != NULL)
					{
						if (IsA($3, List))
						{
							$$ = list_concat($1, (List*)$3);
						}
						else
						{
						$$ = lappend($1, $3);
						}
					}
					else
						$$ = $1;
				}
			| tsql_stmtmulti ';' END_OF_INPUT tsql_stmt
				{
					if ($4 != NULL)
					{
						if (IsA($4, List))
						{
							$$ = list_concat($1, (List*)$4);
						}
						else
						{
						$$ = lappend($1, $4);
						}
					}
					else
						$$ = $1;
				}
			| tsql_stmtmulti END_OF_INPUT_COLON tsql_stmt
				{
					if ($3 != NULL)
					{
						if (IsA($3, List))
						{
							$$ = list_concat($1, (List*)$3);
						}
						else
						{
						$$ = lappend($1, $3);
						}
					}
					else
						$$ = $1;
				}
			|
			{
                #ifndef ENABLE_MULTIPLE_NODES
                	if (u_sess->attr.attr_common.plsql_show_all_error &&
						CompileWhich() != PLPGSQL_COMPILE_NULL) {
                        errstate = NOTICE;
                    } else {
                        errstate = ERROR;
                    }
                #else
                    errstate = ERROR;
                #endif 
			}
			tsql_stmt
				{
					if ($2 != NULL)
					{
						if (IsA($2, List))
						{
							$$ = (List*)$2;
						}
						else
						{
						$$ = list_make1($2);
						}
					}
					else
						$$ = NIL;
				}
		;

tsql_opt_columnstore:
			TSQL_COLUMNSTORE
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The COLUMNSTORE option is currently ignored")));
			}
			| /*EMPTY*/		{ $$ == NULL;}
		;

tsql_opt_clustered:
			TSQL_NONCLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The NONCLUSTERED option is currently ignored")));
			}
			| TSQL_CLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The CLUSTERED option is currently ignored")));
			}
			| /*EMPTY*/		{ $$ == NULL;}
		;

opt_with_no_infomsgs: WITH NO_INFOMSGS				{$$ = TRUE;}
			| /*EMPTY*/								{$$ = FALSE;}
		;		

tsql_unique_clustered:
			TSQL_UNIQUE_NONCLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The NONCLUSTERED option is currently ignored")));
			}
			| TSQL_UNIQUE_CLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The CLUSTERED option is currently ignored")));
			}
		;

tsql_primary_key_clustered:
			TSQL_PRIMAY_KEY_NONCLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The NONCLUSTERED option is currently ignored")));
			}
			| TSQL_PRIMAY_KEY_CLUSTERED
			{
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The CLUSTERED option is currently ignored")));
			}
		;

tsql_opt_unique_clustered:
			tsql_unique_clustered		{ $$ = TRUE; }
			| tsql_opt_clustered		{ $$ = FALSE; }
			| UNIQUE					{ $$ = TRUE; }
		;
	

tsql_ident:
            IDENT
            {
                if (!DB_IS_CMPT(D_FORMAT)) {
                    ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("This syntax is only valid in D-format database."),
                        parser_errposition(@1)));
                }
            }
        ;
tsql_IndexStmt:
				CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
				ON qualified_name access_method_clause '(' index_params ')'
				opt_include opt_reloptions OptPartitionElement opt_table_index_options where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $5;
					n->missing_ok = false;
					n->schemaname = $6->schemaname;
					n->idxname = $6->relname;
					n->relation = $8;
					n->accessMethod = $9;
					n->indexParams = $11;
					n->indexIncludingParams = $13;
					n->options = $14;
					n->tableSpace = $15;
					n->indexOptions = $16;
					n->whereClause = $17;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->partClause = NULL;
					n->isPartitioned = false;
					n->isGlobal = false;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
				| CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_table_index_options
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $5;
					n->missing_ok = false;
					n->schemaname = $6->schemaname;
					n->idxname = $6->relname;
					n->relation = $8;
					n->accessMethod = $9;
					n->indexParams = $11;
					n->partClause  = $14;
					n->indexIncludingParams = $15;
					n->options = $16;
					n->tableSpace = $17;
					n->indexOptions = $18;
					n->isPartitioned = true;
					n->isGlobal = false;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;

				}
				| CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					GLOBAL opt_include opt_reloptions OptTableSpace opt_table_index_options
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $5;
					n->missing_ok = false;
					n->schemaname = $6->schemaname;
					n->idxname = $6->relname;
					n->relation = $8;
					n->accessMethod = $9;
					n->indexParams = $11;
					n->partClause  = NULL;
					n->indexIncludingParams = $14;
					n->options = $15;
					n->tableSpace = $16;
					n->indexOptions = $17;
					n->isPartitioned = true;
					n->isGlobal = true;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;

				}
				| CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					opt_include opt_reloptions OptPartitionElement opt_index_options where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $5;
					n->missing_ok = true;
					n->schemaname = $9->schemaname;
					n->idxname = $9->relname;
					n->relation = $11;
					n->accessMethod = $12;
					n->indexParams = $14;
					n->indexIncludingParams = $16;
					n->options = $17;
					n->tableSpace = $18;
					n->indexOptions = $19;
					n->whereClause = $20;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->partClause = NULL;
					n->isPartitioned = false;
					n->isGlobal = false;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
				| CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_index_options
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $5;
					n->missing_ok = true;
					n->schemaname = $9->schemaname;
					n->idxname = $9->relname;
					n->relation = $11;
					n->accessMethod = $12;
					n->indexParams = $14;
					n->partClause  = $17;
					n->indexIncludingParams = $18;
					n->options = $19;
					n->tableSpace = $20;
					n->indexOptions = $21;
					n->isPartitioned = true;
					n->isGlobal = false;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
				| CREATE tsql_opt_unique_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					GLOBAL opt_include opt_reloptions OptTableSpace opt_index_options
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->missing_ok = true;
					n->unique = $2;
					n->concurrent = $5;
					n->schemaname = $9->schemaname;
					n->idxname = $9->relname;
					n->relation = $11;
					n->accessMethod = $12;
					n->indexParams = $14;
					n->partClause  = NULL;
					n->indexIncludingParams = $17;
					n->options = $18;
					n->tableSpace = $19;
					n->indexOptions = $20;
					n->isPartitioned = true;
					n->isGlobal = true;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
		;

tsql_CreateProcedureStmt:
			CREATE opt_or_replace definer_user procedure_or_proc func_name_opt_arg proc_args
			opt_createproc_opt_list as_is {
				u_sess->parser_cxt.eaten_declare = false;
				u_sess->parser_cxt.eaten_begin = false;
				pg_yyget_extra(yyscanner)->core_yy_extra.include_ora_comment = true;
                u_sess->parser_cxt.isCreateFuncOrProc = true;
				if (set_is_create_plsql_type()) {
					set_create_plsql_type_start();
					set_function_style_a();
				}
			} tsql_subprogram_body
				{
                                        int rc = 0;
                                        rc = CompileWhich();
                                        if ((rc == PLPGSQL_COMPILE_PROC || rc == PLPGSQL_COMPILE_NULL) && u_sess->cmd_cxt.CurrentExtensionObject == InvalidOid) {
                                            u_sess->plsql_cxt.procedure_first_line = GetLineNumber(t_thrd.postgres_cxt.debug_query_string, @8);
                                        }
					rc = CompileWhich();
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					FunctionSources *funcSource = (FunctionSources *)$10;
					int count = get_outarg_num($6);

					n->isOraStyle = true;
					n->isPrivate = false;
					n->replace = $2;
					n->definer = $3;
					if (n->replace && NULL != n->definer) {
						parser_yyerror("not support DEFINER function");
					}
					n->funcname = $5;
					n->parameters = $6;
					n->inputHeaderSrc = FormatFuncArgType(yyscanner, funcSource->headerSrc, n->parameters);
					if (enable_plpgsql_gsdependency_guc()) {
						n->funcHeadSrc = ParseFuncHeadSrc(yyscanner, false);
					}
					n->returnType = NULL;
					n->isProcedure = true;
					if (0 == count)
					{
						n->returnType = makeTypeName("void");
						n->returnType->typmods = NULL;
						n->returnType->arrayBounds = NULL;
					}
					n->options = $7;
					n->options = lappend(n->options, makeDefElem("as",
										(Node *)list_make1(makeString(funcSource->bodySrc))));
					n->options = lappend(n->options, makeDefElem("language",
										(Node *)makeString("pltsql")));
					n->withClause = NIL;
                    u_sess->parser_cxt.isCreateFuncOrProc = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace definer_user procedure_or_proc func_name_opt_arg proc_args
                       LANGUAGE ColId_or_Sconst AS func_as opt_createproc_opt_list
                               {
                                       CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
                                       int count = get_outarg_num($6);
                                       n->isOraStyle = false;
                                       n->isPrivate = false;
                                       n->replace = $2;
                                       n->definer = $3;
                                       if (n->replace && NULL != n->definer) {
                                               parser_yyerror("not support DEFINER function");
                                       }
                                       n->funcname = $5;
                                       n->parameters = $6;
                                       n->returnType = NULL;
                                       if (0 == count)
                                       {
                                               n->returnType = makeTypeName("void");
                                               n->returnType->typmods = NULL;
                                               n->returnType->arrayBounds = NULL;
                                       }
                                       n->options = $11;
                                       n->options = lappend(n->options, makeDefElem("language", (Node *)makeString($8)));
                                       n->options = lappend(n->options, makeDefElem("as", (Node *)$10));
                                       n->withClause = NIL;
                                       n->isProcedure = true;
                                       $$ = (Node *)n;
							   }
		;

procedure_or_proc:     PROCEDURE
                       | TSQL_PROC
				;
tsql_subprogram_body:        {
                                int             proc_b  = 0;
                                int             proc_e  = 0;
                                char    *proc_body_str  = NULL;
                                int             proc_body_len   = 0;
                                int             blocklevel              = 0;
                                bool    add_declare             = true;  /* Mark if need to add a DECLARE */
                                FunctionSources *funSrc = NULL;
                                char *proc_header_str = NULL;
                                int rc = 0;
                                rc = CompileWhich();
                                int     tok = YYEMPTY;
                                int     pre_tok = 0;
                                int in_procedure = 0;
                                int max_proc_level = 0;
                                bool in_begin = false;
                                base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
                                int as_count = 0;
                                int procedure_count = 0;

                                yyextra->core_yy_extra.in_slash_proc_body = true;
                                if (u_sess->parser_cxt.eaten_begin)
                                        blocklevel = 1;

                                if (yychar == YYEOF || yychar == YYEMPTY)
                                        tok = YYLEX;
                                else
                                {
                                        tok = yychar;
                                        yychar = YYEMPTY;
                                }

                                if (u_sess->parser_cxt.eaten_declare || DECLARE == tok)
                                        add_declare = false;

                                proc_header_str = ParseFunctionArgSrc(yyscanner);

                                proc_b = yylloc;
                                if (rc != PLPGSQL_COMPILE_NULL && rc != PLPGSQL_COMPILE_PROC) {
                                        u_sess->plsql_cxt.procedure_first_line = GetLineNumber(yyextra->core_yy_extra.scanbuf, yylloc);
                                }
                                while(true)
                                {
                                        if (tok == YYEOF) {
                                                proc_e = yylloc;
                                                parser_yyerror("subprogram body is not ended correctly");
                                                break;
                                        }
                                        if (!in_begin && (pre_tok == ';' || pre_tok == DECLARE || pre_tok == 0 || pre_tok == COMMENTSTRING
                                                || pre_tok == AS || pre_tok == IS) && (tok == PROCEDURE || tok == FUNCTION)) {
                                                in_procedure++;
                                                max_proc_level = max_proc_level > in_procedure ? max_proc_level : in_procedure;
                                                procedure_count++;
                                        }
                                        if (tok == BEGIN_P) {
                                                pre_tok = tok;
                                                tok = YYLEX;
                                                if (tok != TRY && tok != CATCH) {
                                                    blocklevel++;
                                                    in_begin = true;
                                                } else {
                                                    continue;
                                                }
                                        }
                                        if (tok == AS || tok == IS) {
                                                as_count++;
                                        }
                                        if (tok == END_P)
                                        {
                                                tok = YYLEX;

                                                if (!(tok == ';' || (tok == 0 || tok == END_OF_PROC))
                                                        && tok != IF_P
                                                        && tok != CASE
                                                        && tok != LOOP
                                                        && tok != WHILE_P
                                                        && tok != REPEAT
                                                        && tok != TRY
                                                        && tok != CATCH)
                                                {
                                                        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && blocklevel == 1 && pre_tok == ';' && as_count == 0 && procedure_count ==0)
                                                        {
                                                                proc_e = yylloc;
                                                                break;
                                                        }
                                                        tok = END_P;
                                                        continue;
                                                }

                                                if (blocklevel == 1
                                                        && (pre_tok == ';' || pre_tok == BEGIN_P)
                                                        && (tok == ';' || (tok == 0 || tok == END_OF_PROC)))
                                                {
                                                        proc_e = yylloc;

                                                        if (tok == ';' )
                                                        {
                                                                if (yyextra->lookahead_len != 0) {
                                                                        parser_yyerror("subprogram body is not ended correctly");
                                                                        break;
                                                                }
                                                                else
                                                                {
                                                                        const YYLTYPE yyleng = pg_yyget_leng(yyscanner);
                                                                        yyextra->lookaheads[0] = {
                                                                                .token = tok,
                                                                                .yylloc = yylloc,
                                                                                .yyleng = yyleng,
                                                                                .prev_hold_char_loc = yylloc + yyleng,
                                                                                .prev_hold_char = yyextra->core_yy_extra.scanbuf[yylloc + yyleng],
                                                                        };
                                                                        yyextra->lookahead_len = 1;
                                                                }
                                                        }
                                                        if(in_procedure == 0)
                                                                break;
                                                        else {
                                                                blocklevel--;
                                                                in_procedure--;
                                                                if ((procedure_count - as_count - 1) == in_procedure) {
                                                                        break;
                                                                }
                                                        }
                                                }

                                                if (blocklevel > 1
                                                         && (pre_tok == ';' || pre_tok == BEGIN_P)
                                                         && (tok == ';' || tok == 0))
                                                {
                                                        blocklevel--;
                                                }
                                                in_begin = false;
                                        }

                                        pre_tok = tok;
                                        tok = YYLEX;

                                }

                                if (proc_e == 0) {
                                        ereport(errstate, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subprogram body is not ended correctly")));
                                }
                                if (max_proc_level > 0) {
                                        u_sess->parser_cxt.has_subprogram = true;
                                } else {
                                        u_sess->parser_cxt.has_subprogram = false;
                                }

                                proc_body_len = proc_e - proc_b + 1 ;
                                if (add_declare)
                                {
                                        proc_body_str = (char *)palloc0(proc_body_len + DECLARE_LEN + 1);
                                        rc = strcpy_s(proc_body_str, proc_body_len + DECLARE_LEN + 1, DECLARE_STR);
                                        securec_check(rc, "", "");
                                        rc = strncpy_s(proc_body_str + DECLARE_LEN, proc_body_len + 1,
                                                        yyextra->core_yy_extra.scanbuf + proc_b, proc_body_len - 1);
                                        securec_check(rc, "", "");
                                        proc_body_len = DECLARE_LEN + proc_body_len;
                                }
                                else
                                {
                                        proc_body_str = (char *)palloc0(proc_body_len + 1);
                                        rc = strncpy_s(proc_body_str, proc_body_len + 1,
                                                yyextra->core_yy_extra.scanbuf + proc_b, proc_body_len - 1);
                                        securec_check(rc, "", "");
                                }

                                proc_body_str[proc_body_len] = '\0';

                                yyextra->core_yy_extra.in_slash_proc_body = false;
                                yyextra->core_yy_extra.dolqstart = NULL;

                                yyextra->core_yy_extra.query_string_locationlist =
                                        lappend_int(yyextra->core_yy_extra.query_string_locationlist, yylloc);

                                funSrc = makeNode(FunctionSources);
                                funSrc->bodySrc   = proc_body_str;
                                funSrc->headerSrc = proc_header_str;

                                $$ = funSrc;
                        }
                  ;
ColConstraintElem:     IDENTITY_P identity_seed_increment
                            {
                                Constraint *n = makeNode(Constraint);
                                n->contype = CONSTR_IDENTITY;
                                n->generated_when = ATTRIBUTE_IDENTITY_ALWAYS;
                                n->options = $2;
                                n->location = @1;
                                $$ = (Node *)n;
                            }
						| opt_unique_key opt_definition OptConsTableSpaceWithEmpty InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_UNIQUE;
								n->location = @1;
								n->keys = NULL;
								n->options = $2;
								n->indexname = NULL;
								n->indexspace = $3;
								n->inforConstraint = (InformationalConstraint *) $4;
								n->initially_valid = true;
								$$ = (Node *)n;
							}
						| opt_unique_key opt_definition OptConsTableSpaceWithEmpty ENABLE_P ConstraintAttr_isValidate InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_UNIQUE;
								n->location = @1;
								n->keys = NULL;
								n->options = $2;
								n->indexname = NULL;
								n->indexspace = $3;

								int cas_type = 0;
								if ($5 == CAS_NO_VALIDATE)
									cas_type = CAS_NOT_VALID;
								processCASbits(cas_type, @5, "UNIQUE",
												&n->deferrable, &n->initdeferred, &n->skip_validation,
												NULL, yyscanner);
								n->initially_valid = !n->skip_validation;
								n->isdisable = false;
								n->inforConstraint = (InformationalConstraint *) $6;
								$$ = (Node *)n;
							}
						| opt_unique_key opt_definition OptConsTableSpaceWithEmpty DISABLE_P ConstraintAttr_isValidate InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_UNIQUE;
								n->location = @1;
								n->keys = NULL;
								n->options = $2;
								n->indexname = NULL;
								n->indexspace = $3;
								
								int cas_type = 0;
								if ($5 == CAS_VALIDATE)
									cas_type = CAS_DISABLE_VALIDATE;
								else
									cas_type = CAS_DISABLE_NO_VALIDATE;
								processCASbits(cas_type, @5, "UNIQUE",
												&n->deferrable, &n->initdeferred, &n->skip_validation,
												NULL, yyscanner);
								n->initially_valid = !n->skip_validation;
								n->isdisable = true;
								n->inforConstraint = (InformationalConstraint *) $6;
								$$ = (Node *)n;
							}
						| PRIMARY KEY opt_definition OptConsTableSpaceWithEmpty InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_PRIMARY;
								n->location = @1;
								n->keys = NULL;
								n->options = $3;
								n->indexname = NULL;
								n->indexspace = $4;
								n->inforConstraint = (InformationalConstraint *) $5;
								n->initially_valid = true;
								$$ = (Node *)n;
							}
						| PRIMARY KEY opt_definition OptConsTableSpaceWithEmpty ENABLE_P ConstraintAttr_isValidate InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_PRIMARY;
								n->location = @1;
								n->keys = NULL;
								n->options = $3;
								n->indexname = NULL;
								n->indexspace = $4;
								
								int cas_type = 0;
								if ($6 == CAS_NO_VALIDATE)
									cas_type = CAS_NOT_VALID;
								processCASbits(cas_type, @6, "PRIMARY KEY",
												&n->deferrable, &n->initdeferred, &n->skip_validation,
												NULL, yyscanner);
								n->initially_valid = !n->skip_validation;
								n->isdisable = false;
								n->inforConstraint = (InformationalConstraint *) $7;
								$$ = (Node *)n;
							}
						| PRIMARY KEY opt_definition OptConsTableSpaceWithEmpty DISABLE_P ConstraintAttr_isValidate InformationalConstraintElem FileGroup
							{
								Constraint *n = makeNode(Constraint);
								n->contype = CONSTR_PRIMARY;
								n->location = @1;
								n->keys = NULL;
								n->options = $3;
								n->indexname = NULL;
								n->indexspace = $4;
								
								int cas_type = 0;
								if ($6 == CAS_VALIDATE)
									cas_type = CAS_DISABLE_VALIDATE;
								else
									cas_type = CAS_DISABLE_NO_VALIDATE;
								processCASbits(cas_type, @6, "PRIMARY KEY",
												&n->deferrable, &n->initdeferred, &n->skip_validation,
												NULL, yyscanner);
								n->initially_valid = !n->skip_validation;
								n->isdisable = true;
								n->inforConstraint = (InformationalConstraint *) $7;
								$$ = (Node *)n;
							}
					;

identity_seed_increment:
                       '(' NumericOnly ',' NumericOnly ')'
                       {
                           $$ = list_make2(makeDefElem("start", (Node *)$2), makeDefElem("increment",(Node *)$4));
                       }
                       | /* EMPTY */
                       {
                           $$ = list_make2(makeDefElem("start", (Node *)makeInteger(1)), makeDefElem("increment", (Node *)makeInteger(1)));
                       }
                       ;
rotate_clause:
		ROTATE '(' func_application_list rotate_for_clause rotate_in_clause ')' alias_clause %prec ROTATE
			{
				RotateClause *n = makeNode(RotateClause);
				n->aggregateFuncCallList = $3;
				n->forColName = $4;
				n->inExprList = $5;
				base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
				char* raw_parse_query_string = yyextra->core_yy_extra.scanbuf;
				n->inExprList = TransformToConstStrNode(n->inExprList, raw_parse_query_string);
				n->alias = $7;
				$$ = n;
			}
		;

unrotate_clause:
			NOT ROTATE include_exclude_null_clause '(' unrotate_name_list rotate_for_clause unrotate_in_clause ')' alias_clause %prec ROTATE
				{
					UnrotateClause *n = makeNode(UnrotateClause);
					n->includeNull = $3;
					n->colNameList = $5;
					n->forColName = $6;
					n->inExprList = $7;
					n->alias = $9;
					$$ = n;
				}
		;

VariableSetStmt:
			SET IDENT var_value
			    {
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $2;
					n->args = list_make1($3);
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

unreserved_keyword:
			CHECKIDENT
			| DBCC
			| NO_INFOMSGS
			| NORESEED
			| RESEED
			| TSQL_COLUMNSTORE
			| TSQL_CLUSTERED
			| TSQL_NONCLUSTERED
			| TSQL_PERSISTED
			| TSQL_NOLOCK
			| TSQL_READUNCOMMITTED
			| TSQL_UPDLOCK
			| TSQL_REPEATABLEREAD
			| TSQL_READCOMMITTED
			| TSQL_TABLOCK
			| TSQL_TABLOCKX
			| TSQL_PAGLOCK
			| TSQL_ROWLOCK
			| TSQL_READPAST
			| TSQL_XLOCK
			| TSQL_NOEXPAND
			| TSQL_PROC 
			| TSQL_MINUTES_P
			| TSQL_TEXTIMAGE_ON
			| TSQL_D
			| TSQL_DAYOFYEAR
			| TSQL_DW
			| TSQL_DY
			| TSQL_HH
			| TSQL_M
			| TSQL_MCS
			| TSQL_MI
			| TSQL_MICROSECOND
			| TSQL_MILLISECOND
			| TSQL_MM
			| TSQL_MS
			| TSQL_N
			| TSQL_NS
			| TSQL_Q
			| TSQL_QQ
			| TSQL_QUARTER
			| TSQL_SS
			| TSQL_S
			| TSQL_WEEK
			| TSQL_WEEKDAY
			| TSQL_WK
			| TSQL_WW
			| TSQL_W
			| TSQL_Y
			| TSQL_YYYY
			| TSQL_YY
			| TSQL_DD
			| TSQL_NANOSECOND ;

reserved_keyword:
			TSQL_TRY_CAST
			| TSQL_TRY_CONVERT
			| TSQL_CONVERT
			| TSQL_DATEDIFF
			| TSQL_DATEDIFF_BIG ;

DBCCCheckIdentStmt:
		DBCC CHECKIDENT '(' ColId_or_Sconst ',' NORESEED ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_no_reseed_func(quote_identifier_wrapper($4, yyscanner), $8, false);
				n->intoClause = NULL;
				n->fromClause = NIL;
				n->whereClause = NULL;
				n->groupClause = NIL;
				n->havingClause = NULL;
				n->windowClause = NIL;
				$$ = (Node*)n;
			}
		| DBCC CHECKIDENT '(' ColId_or_Sconst ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_no_reseed_func(quote_identifier_wrapper($4, yyscanner), $6, true);
				n->intoClause = NULL;
				n->fromClause = NIL;
				n->whereClause = NULL;
				n->groupClause = NIL;
				n->havingClause = NULL;
				n->windowClause = NIL;
				$$ = (Node*)n;
			}
		| DBCC CHECKIDENT '(' ColId_or_Sconst ',' RESEED ',' NumericOnly ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_reseed_func(quote_identifier_wrapper($4, yyscanner), makeAConst($8, @8), $10);
				n->intoClause = NULL;
				n->fromClause = NIL;
				n->whereClause = NULL;
				n->groupClause = NIL;
				n->havingClause = NULL;
				n->windowClause = NIL;
				$$ = (Node*)n;
			}
		| DBCC CHECKIDENT '(' ColId_or_Sconst ',' RESEED ',' columnref ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_reseed_func(quote_identifier_wrapper($4, yyscanner), $8, $10);
				n->intoClause = NULL;
				n->fromClause = NIL;
				n->whereClause = NULL;
				n->groupClause = NIL;
				n->havingClause = NULL;
				n->windowClause = NIL;
				$$ = (Node*)n;
			}
		| DBCC CHECKIDENT '(' ColId_or_Sconst ',' RESEED ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_no_reseed_func(quote_identifier_wrapper($4, yyscanner), $8, true);
				n->intoClause = NULL;
				n->fromClause = NIL;
				n->whereClause = NULL;
				n->groupClause = NIL;
				n->havingClause = NULL;
				n->windowClause = NIL;
				$$ = (Node*)n;
			}
		;

DBCCStmt:  DBCCCheckIdentStmt
			{
				$$ = $1;
			}
			;

TSQL_AnonyBlockStmt:
		DECLARE { u_sess->parser_cxt.eaten_declare = true; u_sess->parser_cxt.eaten_begin = false; } tsql_subprogram_body
			{
				$$ = (Node *)TsqlMakeAnonyBlockFuncStmt(DECLARE, ((FunctionSources*)$3)->bodySrc);
			}
		| BEGIN_P { u_sess->parser_cxt.eaten_declare = true; u_sess->parser_cxt.eaten_begin = true; } tsql_subprogram_body
			{
				$$ = (Node *)TsqlMakeAnonyBlockFuncStmt(BEGIN_P, ((FunctionSources*)$3)->bodySrc);
			}
		;


TSQL_CreateFunctionStmt:
			CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args
			RETURNS func_return createfunc_opt_list opt_definition
				{
					set_function_style_pg();
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->isPrivate = false;
					n->replace = $2;
					n->definer = $3;
					if (n->replace && NULL != n->definer) {
						parser_yyerror("not support DEFINER function");
					}			
					n->funcname = $5;
					n->parameters = $6;
					n->returnType = $8;
					if (list_length($8->names) == 1 && pg_strcasecmp(strVal(linitial($8->names)), "trigger") == 0) {
						DefElem* language_item = NULL;
						ListCell* option = NULL;
						foreach (option, $9) {
							DefElem* defel = (DefElem*)lfirst(option);
							if (pg_strcasecmp(defel->defname, "language") == 0) {
								language_item = defel;
								char* language = strVal(defel->arg);
								if (pg_strcasecmp(language, "plpgsql") == 0) {
									defel->arg = (Node *) makeString("pltsql");
								}
								break;
							}
						}
						if (language_item) {
							$9 = list_delete($9, language_item);
						}
						$9 = lappend($9, makeDefElem("language", (Node *)makeString("pltsql")));
					}
					n->options = $9;
					n->withClause = $10;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args
			  RETURNS TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
				{
					set_function_style_pg();
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->isPrivate = false;
					n->replace = $2;
					n->definer = $3;
					if (n->replace && NULL != n->definer) {
						parser_yyerror("not support DEFINER function");
					}
					n->funcname = $5;
					n->parameters = mergeTableFuncParameters($6, $10);
					n->returnType = TableFuncTypeName($10);
					n->returnType->location = @8;
					n->options = $12;
					n->withClause = $13;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args
			  createfunc_opt_list opt_definition
				{
					set_function_style_pg();
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->isPrivate = false;
					n->replace = $2;
					n->definer = $3;
					if (n->replace && NULL != n->definer) {
						parser_yyerror("not support DEFINER function");
					}
					n->funcname = $5;
					n->parameters = $6;
					n->returnType = NULL;
					n->options = $7;
					n->withClause = $8;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args
			  RETURN func_return opt_createproc_opt_list as_is {
				  u_sess->parser_cxt.eaten_declare = false;
				  u_sess->parser_cxt.eaten_begin = false;
				  pg_yyget_extra(yyscanner)->core_yy_extra.include_ora_comment = true;
				  u_sess->parser_cxt.isCreateFuncOrProc = true;
				  if (set_is_create_plsql_type()) {
					set_create_plsql_type_start();
					set_function_style_a();
				  }
			  } tsql_subprogram_body
				{
					int rc = 0;
					rc = CompileWhich();
					if (rc == PLPGSQL_COMPILE_PROC || rc == PLPGSQL_COMPILE_NULL) {
						u_sess->plsql_cxt.procedure_first_line = GetLineNumber(t_thrd.postgres_cxt.debug_query_string, @10);
					}
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					FunctionSources *funcSource = (FunctionSources *)$12;
					n->isOraStyle = true;
					n->isPrivate = false;
					n->replace = $2;
					n->definer = $3;
					if (n->replace && NULL != n->definer) {
						parser_yyerror("not support DEFINER function");
					}
					n->funcname = $5;
					n->parameters = $6;
					n->inputHeaderSrc = FormatFuncArgType(yyscanner, funcSource->headerSrc, n->parameters);
					if (enable_plpgsql_gsdependency_guc()) {
						n->funcHeadSrc = ParseFuncHeadSrc(yyscanner);
					}
					n->returnType = $8;
					n->options = $9;
					n->options = lappend(n->options, makeDefElem("as",
										(Node *)list_make1(makeString(funcSource->bodySrc))));
					n->options = lappend(n->options, makeDefElem("language",
										(Node *)makeString("pltsql")));

					n->withClause = NIL;
					n->withClause = NIL;
					n->isProcedure = false;
					u_sess->parser_cxt.isCreateFuncOrProc = false;
					$$ = (Node *)n;
				}
		;

TSQL_DoStmt: DO dostmt_opt_list
				{
					DoStmt *n = makeNode(DoStmt);
					bool with_language = false;
					n->args = $2;
					ListCell* arg = NULL;
					foreach (arg, n->args) {
						DefElem* defel = (DefElem*)lfirst(arg);
						if (strcmp(defel->defname, "language") == 0) {
							with_language = true;
							break;
						}
					}
					if (!with_language) {
						n->args = lappend(n->args, makeDefElem("language", (Node *)makeString("pltsql")));
					}
					$$ = (Node *)n;
				}
		;

ConstraintElem:
			tsql_unique_clustered '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options
				ConstraintAttributeSpec InformationalConstraintElem OptFileGroup
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->including = $5;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = NULL;
					n->constraintOptions = $7;
					processCASbits($8, @8, "UNIQUE",
								   &n->deferrable, &n->initdeferred, &n->skip_validation,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $9; /* informational constraint info */
					n->initially_valid = !n->skip_validation;
					if ($8 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
						n->isdisable = true;
					setAccessMethod(n);
					$$ = (Node *)n;
				}
				| tsql_primary_key_clustered '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options
				ConstraintAttributeSpec InformationalConstraintElem OptFileGroup
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $3;
					n->including = $5;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = NULL;
					n->constraintOptions = $7;
					processCASbits($8, @8, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, &n->skip_validation,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $9; /* informational constraint info */
					n->initially_valid = !n->skip_validation;
					if ($8 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
						n->isdisable = true;
					setAccessMethod(n);
					$$ = (Node *)n;
				}
				| UNIQUE '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options
				ConstraintAttributeSpec InformationalConstraintElem FileGroup
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->including = $5;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					n->constraintOptions = $8;
					processCASbits($9, @9, "UNIQUE",
								   &n->deferrable, &n->initdeferred, &n->skip_validation,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $10; /* informational constraint info */
					n->initially_valid = !n->skip_validation;
					if ($9 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
						n->isdisable = true;
					setAccessMethod(n);
					$$ = (Node *)n;
				}
				| UNIQUE '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options
					ConstraintAttributeSpec InformationalConstraintElem FileGroup
					{
						Constraint *n = makeNode(Constraint);
						n->contype = CONSTR_UNIQUE;
						n->location = @1;
						n->keys = $3;
						n->including = $5;
						n->options = $6;
						n->indexname = NULL;
						n->indexspace = NULL;
						n->constraintOptions = $7;
						processCASbits($8, @8, "UNIQUE",
									&n->deferrable, &n->initdeferred, &n->skip_validation,
									NULL, yyscanner);
						n->inforConstraint = (InformationalConstraint *) $9; /* informational constraint info */
						n->initially_valid = !n->skip_validation;
						if ($8 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
							n->isdisable = true;
						setAccessMethod(n);
						$$ = (Node *)n;
					}
				| PRIMARY KEY '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options
					ConstraintAttributeSpec InformationalConstraintElem FileGroup
					{
						Constraint *n = makeNode(Constraint);
						n->contype = CONSTR_PRIMARY;
						n->location = @1;
						n->keys = $4;
						n->including = $6;
						n->options = $7;
						n->indexname = NULL;
						n->indexspace = $8;
						n->constraintOptions = $9;
						processCASbits($10, @10, "PRIMARY KEY",
									&n->deferrable, &n->initdeferred, &n->skip_validation,
									NULL, yyscanner);
						n->inforConstraint = (InformationalConstraint *) $11; /* informational constraint info */
						n->initially_valid = !n->skip_validation;
						if ($10 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
							n->isdisable = true;
						setAccessMethod(n);
						$$ = (Node *)n;
					}
				| PRIMARY KEY '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options
					ConstraintAttributeSpec InformationalConstraintElem FileGroup
					{
						Constraint *n = makeNode(Constraint);
						n->contype = CONSTR_PRIMARY;
						n->location = @1;
						n->keys = $4;
						n->including = $6;
						n->options = $7;
						n->indexname = NULL;
						n->indexspace = NULL;
						n->constraintOptions = $8;
						processCASbits($9, @9, "PRIMARY KEY",
									&n->deferrable, &n->initdeferred, &n->skip_validation,
									NULL, yyscanner);
						n->inforConstraint = (InformationalConstraint *) $10; /* informational constraint info */
						n->initially_valid = !n->skip_validation;
						if ($9 & (CAS_DISABLE_VALIDATE | CAS_DISABLE_NO_VALIDATE))
							n->isdisable = true;
						setAccessMethod(n);
						$$ = (Node *)n;
					}
			;

tsql_stmt :
			AlterAppWorkloadGroupMappingStmt
			| AlterCoordinatorStmt
			| AlterDatabaseStmt
			| AlterDatabaseSetStmt
			| AlterDataSourceStmt
			| AlterDefaultPrivilegesStmt
			| AlterDomainStmt
			| AlterEnumStmt
			| AlterEventTrigStmt
			| AlterExtensionStmt
			| AlterExtensionContentsStmt
			| AlterFdwStmt
			| AlterForeignServerStmt
			| AlterForeignTableStmt
			| AlterFunctionStmt
			| AlterProcedureStmt
			| AlterPublicationStmt
			| AlterGroupStmt
			| AlterNodeGroupStmt
			| AlterNodeStmt
			| AlterObjectSchemaStmt
			| AlterOwnerStmt
			| AlterRlsPolicyStmt
			| AlterResourcePoolStmt
			| AlterGlobalConfigStmt
			| AlterSeqStmt
			| AlterSchemaStmt
			| AlterSubscriptionStmt
			| AlterTableStmt
			| AlterSystemStmt
			| AlterTriggerStmt
			| AlterCompositeTypeStmt
			| AlterRoleSetStmt
			| AlterRoleStmt
			| AlterSessionStmt
			| AlterTSConfigurationStmt
			| AlterTSDictionaryStmt
			| AlterUserMappingStmt
			| AlterUserSetStmt
			| AlterUserStmt
			| AlterWorkloadGroupStmt
			| AnalyzeStmt
			| TSQL_AnonyBlockStmt
			| BarrierStmt
			| CreateAppWorkloadGroupMappingStmt
			| CallFuncStmt
			| CheckPointStmt
			| CleanConnStmt
			| ClosePortalStmt
			| ClusterStmt
			| CommentStmt
			| CompileStmt
			| ConstraintsSetStmt
			| CopyStmt
			| CreateAmStmt
			| CreateAsStmt
			| CreateAssertStmt
			| CreateCastStmt
			| CreateContQueryStmt
			| CreateStreamStmt
			| CreateConversionStmt
			| CreateDomainStmt
			| CreateDirectoryStmt
			| CreateEventTrigStmt
			| CreateExtensionStmt
			| CreateFdwStmt
			| CreateForeignServerStmt
			| CreateForeignTableStmt
			| CreateDataSourceStmt
			| TSQL_CreateFunctionStmt
			| CreateEventStmt
			| AlterEventStmt
			| DropEventStmt
			| ShowEventStmt
			| CreatePackageStmt
			| CreatePackageBodyStmt
			| CreateGroupStmt
			| CreateMatViewStmt
			| CreateMatViewLogStmt
			| DropMatViewLogStmt
			| CreateModelStmt  // DB4AI
			| CreateNodeGroupStmt
			| CreateNodeStmt
			| CreateOpClassStmt
			| CreateOpFamilyStmt
			| AlterOpFamilyStmt
			| CreateRlsPolicyStmt
			| CreatePLangStmt
			| tsql_CreateProcedureStmt
			| CreatePublicationStmt
            | CreateKeyStmt
			| CreatePolicyLabelStmt
			| CreateWeakPasswordDictionaryStmt
			| DropWeakPasswordDictionaryStmt
			| AlterPolicyLabelStmt
			| DropPolicyLabelStmt
            | CreateAuditPolicyStmt
            | AlterAuditPolicyStmt
            | DropAuditPolicyStmt
			| CreateMaskingPolicyStmt
			| AlterMaskingPolicyStmt
			| DropMaskingPolicyStmt
			| CreateResourcePoolStmt
			| CreateSchemaStmt
			| CreateSeqStmt
			| CreateStmt
			| CreateSubscriptionStmt
			| CreateSynonymStmt
			| CreateTableSpaceStmt
			| CreateTrigStmt
			| CreateRoleStmt
			| CreateUserStmt
			| CreateUserMappingStmt
			| CreateWorkloadGroupStmt
			| CreatedbStmt
			| DeallocateStmt
			| DeclareCursorStmt
			| DefineStmt
			| DeleteStmt
			| DiscardStmt
			| TSQL_DoStmt
			| DropAppWorkloadGroupMappingStmt
			| DropAssertStmt
			| DropCastStmt
			| DropDataSourceStmt
			| DropDirectoryStmt
			| DropFdwStmt
			| DropForeignServerStmt
			| DropGroupStmt
			| DropModelStmt // DB4AI
			| DropNodeGroupStmt
			| DropNodeStmt
			| DropOpClassStmt
			| DropOpFamilyStmt
			| DropOwnedStmt
			| DropRlsPolicyStmt
			| DropPLangStmt
			| DropResourcePoolStmt
			| DropGlobalConfigStmt
			| DropRuleStmt
			| DropStmt
			| DropSubscriptionStmt
			| DropSynonymStmt
			| DropTableSpaceStmt
			| DropTrigStmt
			| DropRoleStmt
			| DropUserStmt
			| DropUserMappingStmt
			| DropWorkloadGroupStmt
			| DropdbStmt
			| ExecuteStmt
			| ExecDirectStmt
			| ExplainStmt
			| FetchStmt
			| GetDiagStmt
			| GrantStmt
			| GrantRoleStmt
			| GrantDbStmt
			| tsql_IndexStmt
			| tsql_InsertStmt
			| ListenStmt
			| RefreshMatViewStmt
			| LoadStmt
			| LockStmt
			| MergeStmt
			| NotifyStmt
			| PrepareStmt
			| PurgeStmt
			| ReassignOwnedStmt
			| ReindexStmt
			| RemoveAggrStmt
			| RemoveFuncStmt
			| RemovePackageStmt
			| RemoveOperStmt
			| RenameStmt
			| RevokeStmt
			| RevokeRoleStmt
			| RevokeDbStmt
			| RuleStmt
			| SecLabelStmt
			| SelectStmt
			| ShutdownStmt
			| TimeCapsuleStmt
			| SnapshotStmt
			| tsql_TransactionStmt
			| TruncateStmt
			| UnlistenStmt
			| UpdateStmt
			| tsql_UseStmt
			| VacuumStmt
			| VariableResetStmt
			| VariableSetStmt
			| VariableMultiSetStmt
			| VariableShowStmt
			| VerifyStmt
			| ViewStmt
			| ShrinkStmt
			| /*EMPTY*/
				{ $$ = NULL; }
			| DelimiterStmt
			| DBCCStmt
		;
func_expr_common_subexpr:
			TSQL_ATAT_IDENT
				{
					int len = strlen($1);
					errno_t rc = EOK;
 
					char *name	= (char *)palloc(len - 1);
					rc = strncpy_s(name, len - 1, $1 + 2, len-2);
					securec_check(rc, "\0", "\0");

					$$ = (Node *)makeFuncCall(TsqlSystemFuncName2(name), NIL, @1);
				}
			| TSQL_TRY_CAST '(' a_expr AS Typename ')'
				{
					$$ = TsqlFunctionTryCast($3, $5, @1);
				}
			| TSQL_CONVERT '(' Typename ',' a_expr ')'
				{
					$$ = TsqlFunctionConvert($3, $5, NULL, false, @1);
				}
			| TSQL_CONVERT '(' Typename ',' a_expr ',' a_expr ')'
				{
					$$ = TsqlFunctionConvert($3, $5, $7, false, @1);
				}
			| TSQL_TRY_CONVERT '(' Typename ',' a_expr ')'
				{
					$$ = TsqlFunctionConvert($3, $5, NULL, true, @1);
				}
			| TSQL_TRY_CONVERT '(' Typename ',' a_expr ',' a_expr ')'
				{
					$$ = TsqlFunctionConvert($3, $5, $7, true, @1);
				}
			| TSQL_DATEDIFF '(' datediff_arg ',' a_expr ',' a_expr ')'
				{
					$$ = (Node *)makeFuncCall(TsqlSystemFuncName2("datediff"),
												list_make3(makeStringConst($3, @3), $5, $7),
												@1);
				}
			| TSQL_DATEDIFF_BIG '(' datediff_arg ',' a_expr ',' a_expr ')'
				{
					$$ = (Node *)makeFuncCall(TsqlSystemFuncName2("datediff_big"),
											   list_make3(makeStringConst($3, @3), $5, $7),
											   @1);
				}
		;

columnDef:
			ColId TSQL_computed_column ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					/*
					 * For computed columns, user doesn't provide a datatype.
					 * But, PG expects a datatype.  Hence, we just assign a
					 * valid datatype temporarily.  Later, we'll evaluate
					 * expression to detect the actual datatype.
					 */
					n->typname = makeTypeName("varchar");
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = NULL;

					$3 = lappend($3, $2);
					SplitColQualList($3, &n->constraints, &n->collClause,  &n->clientLogicColumnRef, yyscanner);

					$$ = (Node *)n;
				}
		;

/*
 * Computed columns uses b_expr not a_expr to avoid conflict with general NOT
 * (used in constraints).  Besides, it seems TSQL doesn't allow AND, NOT, IS
 * IN clauses in the computed column expression.  So, there shouldn't be
 * any issues.
 */
TSQL_computed_column:
				AS b_expr
				{
					if (t_thrd.proc->workingVersionNum < COMPUTED_COLUMNS_VERSION_NUMBER) {
						ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Working Version Num less than %u does not support computed columns.",
									   COMPUTED_COLUMNS_VERSION_NUMBER)));
					}

					ereport(NOTICE,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("The virtual computed columns (non-persisted) are currently ignored and behave the same as persisted columns.")));
					
					Constraint *n = makeNode(Constraint);

					n->contype = CONSTR_GENERATED;
					n->generated_when = ATTRIBUTE_GENERATED_PERSISTED;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					n->location = @1;

					$$ = (Node *)n;
				}
				| AS b_expr TSQL_PERSISTED
				{
					if (t_thrd.proc->workingVersionNum < COMPUTED_COLUMNS_VERSION_NUMBER) {
						ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Working Version Num less than %u does not support computed columns.",
									   COMPUTED_COLUMNS_VERSION_NUMBER)));
					}
					Constraint *n = makeNode(Constraint);

					n->contype = CONSTR_GENERATED;
					n->generated_when = ATTRIBUTE_GENERATED_PERSISTED;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					n->location = @1;

					$$ = (Node *)n;
				}
		;

tsql_select_top_value:
            SignedIconst                        { $$ = makeIntConst($1, @1); }
            | FCONST                             { $$ = makeFloatConst($1, @1); }
            | '(' a_expr ')'                    { $$ = $2; }
            | select_with_parens
                {
                    /*
                     * We need a speical grammar for scalar subquery here
                     * because c_expr (in a_expr) has a rule select_with_parens but we defined the first rule as '(' a_expr ')'.
                     * In other words, the first rule will be hit only when double parenthesis is used like `SELECT TOP ((select 1)) ...`
                     */
                    SubLink *n = makeNode(SubLink);
                    n->subLinkType = EXPR_SUBLINK;
                    n->testexpr = NULL;
                    n->operName = NIL;
                    n->subselect = $1;
                    n->location = @1;
                    $$ = (Node *)n;
                }
            ;

tsql_opt_ties:
            WITH TIES                            { $$ = true; }
            | /*EMPTY*/                            { $$ = false; }
        ;

tsql_opt_percent:
            TSQL_PERCENT                        { $$ = true; }
            | /*EMPTY*/                            { $$ = false; }
        ;

tsql_top_clause:
            TSQL_TOP tsql_select_top_value tsql_opt_percent tsql_opt_ties
                {
                    FetchLimit *result = (FetchLimit *)palloc0(sizeof(FetchLimit));
                    result->limitOffset = NULL;
                    result->limitCount = $2;
                    result->isPercent = $3;
                    result->isWithTies = $4;
                    result->isFetch = true;
                    $$ = (Node *)result;
                }
            ;

simple_select:
            SELECT hint_string opt_distinct tsql_top_clause target_list
            opt_into_clause from_clause where_clause
            group_clause having_clause window_clause
                {
                    SelectStmt *n = makeNode(SelectStmt);
                    n->distinctClause = $3;

                    FetchLimit* topClause = (FetchLimit*)$4;
                    if (n->limitCount) {
                        const char* message = "multiple OFFSET clauses not allowed";
                        InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("multiple LIMIT clauses not allowed"),
                                parser_errposition(exprLocation(topClause->limitCount))));
                    }
                    n->limitCount = topClause->limitCount;
                    n->isFetch = topClause->isFetch;
                    n->limitIsPercent = topClause->isPercent;
                    n->limitWithTies = topClause->isWithTies;

                    n->targetList = $5;
                    n->intoClause = $6;
                    n->fromClause = $7;
                    n->whereClause = $8;
                    n->groupClause = $9;
                    n->havingClause = $10;
                    n->windowClause = $11;
                    n->hintState = create_hintstate($2);
                    n->hasPlus = getOperatorPlusFlag();
                    $$ = (Node *)n;
                }
            ;

/* Direct column label --- names that can be column labels without writing "AS".
 * This classification is orthogonal to the other keyword categories.
 */
DirectColLabel:	IDENT								{ $$ = $1; }
			| direct_label_keyword					{ $$ = pstrdup($1); }
		;

/*
 * While all keywords can be used as column labels when preceded by AS,
 * not all of them can be used as a "direct" column label without AS.
 * Those that can be used as a direct label must be listed here,
 * in addition to appearing in one of the category lists above.
 *
 * Always add a new keyword to this list if possible.  Mark it DIRECT_LABEL
 * in kwlist.h if it is included here, or AS_LABEL if it is not.
 */
direct_label_keyword: ABORT_P
            | ABSOLUTE_P
            | ACCESS
            | ACCOUNT
            | ACTION
            | ADD_P
            | ADMIN
            | AFTER
            | AGGREGATE
            | ALGORITHM
            | ALL
            | ALSO
            | ALTER
            | ALWAYS
            | ANALYSE
            | ANALYZE
            | AND
            | ANY
            | APP
            | APPEND
            | APPLY
            | ARCHIVE
            | ASC
            | ASOF_P
            | ASSERTION
            | ASSIGNMENT
            | ASYMMETRIC
            | AT
            | ATTRIBUTE
            | AUDIT
            | AUTHID
            | AUTHORIZATION
            | AUTO_INCREMENT
            | AUTOEXTEND
            | AUTOMAPPED
            | BACKWARD
            | BARRIER
            | BEFORE
            | BEGIN_P
            | BEGIN_NON_ANOYBLOCK
            | BIGINT
            | BINARY
            | BINARY_DOUBLE
            | BINARY_DOUBLE_INF
            | BINARY_DOUBLE_NAN
            | BINARY_INTEGER
            | BIT
            | BLANKS
            | BLOB_P
            | BLOCKCHAIN
            | BODY_P
            | BOOLEAN_P
            | BOTH
            | BUCKETCNT
            | BUCKETS
            | BUILD
            | BYTE_P
            | BYTEAWITHOUTORDER
            | BYTEAWITHOUTORDERWITHEQUAL
            | CACHE
            | CALL
            | CALLED
            | CANCELABLE
            | CASCADE
            | CASCADED
            | CASE
            | CAST
            | CATALOG_P
            | CATALOG_NAME
            | CHAIN
            | CHANGE
            | CHARACTERISTICS
            | CHARACTERSET
            | CHARSET
            | CHECK
            | CHECKIDENT
            | CHECKPOINT
            | CLASS
            | CLASS_ORIGIN
            | CLEAN
            | CLIENT
            | CLIENT_MASTER_KEY
            | CLIENT_MASTER_KEYS
            | CLOB
            | CLOSE
            | CLUSTER
            | TSQL_CLUSTERED
            | COALESCE
            | COLLATE
            | COLLATION
            | COLUMN
            | COLUMN_ENCRYPTION_KEY
            | COLUMN_ENCRYPTION_KEYS
            | COLUMN_NAME
            | COLUMNS
            | TSQL_COLUMNSTORE
            | COMMENT
            | COMMENTS
            | COMMIT
            | COMMITTED
            | COMPACT
            | COMPATIBLE_ILLEGAL_CHARS
            | COMPILE
            | COMPLETE
            | COMPLETION
            | COMPRESS
            | CONCURRENTLY
            | CONDITION
            | CONFIGURATION
            | CONNECT
            | CONNECTION
            | CONSISTENT
            | CONSTANT
            | CONSTRAINT
            | CONSTRAINT_CATALOG
            | CONSTRAINT_NAME
            | CONSTRAINT_SCHEMA
            | CONSTRAINTS
            | CONSTRUCTOR
            | CONTENT_P
            | CONTINUE_P
            | CONTVIEW
            | CONVERSION_P
            | TSQL_CONVERT
            | COORDINATOR
            | COORDINATORS
            | COPY
            | COST
            | CROSS
            | CSN
            | CSV
            | CUBE
            | CURRENT_P
            | CURRENT_CATALOG
            | CURRENT_DATE
            | CURRENT_ROLE
            | CURRENT_SCHEMA
            | CURRENT_TIME
            | CURRENT_TIMESTAMP
            | CURRENT_USER
            | CURSOR
            | CURSOR_NAME
            | CYCLE
            | DATA_P
            | DATABASE
            | DATAFILE
            | DATANODE
            | DATANODES
            | DATATYPE_CL
            | DATE_P
            | DATE_FORMAT_P
            | DAY_HOUR_P
            | DAY_MINUTE_P
            | DAY_SECOND_P
            | DBCC
            | DBCOMPATIBILITY_P
            | DEALLOCATE
            | DEC
            | DECIMAL_P
            | DECLARE
            | DECODE
            | DEFAULT
            | DEFAULTS
            | DEFERRABLE
            | DEFERRED
            | DEFINER
            | DELETE_P
            | DELIMITER
            | DELIMITERS
            | DELTA
            | DELTAMERGE
            | DENSE_RANK
            | DESC
            | DETERMINISTIC
            | DIAGNOSTICS
            | DICTIONARY
            | DIRECT
            | DIRECTORY
            | DISABLE_P
            | DISCARD
            | DISCONNECT
            | DISTINCT
            | DISTRIBUTE
            | DISTRIBUTION
            | DO
            | DOCUMENT_P
            | DOMAIN_P
            | DOUBLE_P
            | DROP
            | DUMPFILE
            | DUPLICATE
            | EACH
            | ELASTIC
            | ELSE
            | ENABLE_P
            | ENCLOSED
            | ENCODING
            | ENCRYPTED
            | ENCRYPTED_VALUE
            | ENCRYPTION
            | ENCRYPTION_TYPE
            | END_P
            | ENDS
            | ENFORCED
            | ENUM_P
            | EOL
            | ERROR_P
            | ERRORS
            | ESCAPE
            | ESCAPED
            | ESCAPING
            | EVENT
            | EVENTS
            | EVERY
            | EXCHANGE
            | EXCLUDE
            | EXCLUDED
            | EXCLUDING
            | EXCLUSIVE
            | EXECUTE
            | EXISTS
            | EXPIRED_P
            | EXPLAIN
            | EXTENSION
            | EXTERNAL
            | EXTRACT
            | FALSE_P
            | FAMILY
            | FAST
            | FEATURES
            | FENCED
            | FIELDS
            | FILEHEADER_P
            | FILL_MISSING_FIELDS
            | FILLER
            | FINAL
            | FIRST_P
            | FIXED_P
            | FLOAT_P
            | FOLLOWING
            | FOLLOWS_P
            | FORCE
            | FOREIGN
            | FORMATTER
            | FORWARD
            | FREEZE
            | FULL
            | FUNCTION
            | FUNCTIONS
            | GENERATED
            | GET
            | GLOBAL
            | GRANTED
            | GREATEST
            | GROUPING_P
            | GROUPPARENT
            | HANDLER
            | HDFSDIRECTORY
            | HEADER_P
            | HOLD
            | HOUR_MINUTE_P
            | HOUR_SECOND_P
            | IDENTIFIED
            | IDENTITY_P
            | IF_P
            | IGNORE
            | IGNORE_EXTRA_DATA
            | ILIKE
            | IMCSTORED
            | IMMEDIATE
            | IMMUTABLE
            | IMPLICIT_P
            | IN_P
            | INCLUDE
            | INCLUDING
            | INCREMENT
            | INCREMENTAL
            | INDEX
            | INDEXES
            | INFILE
            | INFINITE_P
            | INHERIT
            | INHERITS
            | INITIAL_P
            | INITIALLY
            | INITRANS
            | INLINE_P
            | INNER_P
            | INOUT
            | INPUT_P
            | INSENSITIVE
            | INSERT
            | INSTEAD
            | INT_P
            | INTEGER
            | INTERNAL
            | INTERVAL
            | INVISIBLE
            | INVOKER
            | IP
            | ISOLATION
            | JOIN
            | JSON_EXISTS
            | KEY
            | KEY_PATH
            | KEY_STORE
            | KILL
            | LABEL
            | LANGUAGE
            | LARGE_P
            | LAST_P
            | LATERAL_P
            | LC_COLLATE_P
            | LC_CTYPE_P
            | LEADING
            | LEAKPROOF
            | LEAST
            | LEFT
            | LESS
            | LEVEL
            | LIKE
            | LINES
            | LIST
            | LISTEN
            | LOAD
            | LOCAL
            | LOCALTIME
            | LOCALTIMESTAMP
            | LOCATION
            | LOCK_P
            | LOCKED
            | LOG_P
            | LOGGING
            | LOGIN_ANY
            | LOGIN_FAILURE
            | LOGIN_SUCCESS
            | LOGOUT
            | LOOP
            | MAP
            | MAPPING
            | MASKING
            | MASTER
            | MATCH
            | MATCHED
            | MATERIALIZED
            | MAXEXTENTS
            | MAXSIZE
            | MAXTRANS
            | MAXVALUE
            | MEMBER
            | MERGE
            | MESSAGE_TEXT
            | METHOD
            | MINEXTENTS
			| TSQL_MINUTES_P
            | MINUTE_SECOND_P
            | MINVALUE
            | MODE
            | MODEL
            | MODIFY_P
            | MOVE
            | MOVEMENT
            | MYSQL_ERRNO
            | NAMES
            | NAN_P
            | NATIONAL
            | NATURAL
            | NCHAR
            | NEXT
            | NO
            | NO_INFOMSGS
            | NOCOMPRESS
            | NOCYCLE
            | NODE
			| TSQL_NOEXPAND
			| TSQL_NOLOCK
            | NOLOGGING
            | NOMAXVALUE
            | NOMINVALUE
            | TSQL_NONCLUSTERED
            | NONE
            | NORESEED
            | NOTHING
            | NOTIFY
            | NOVALIDATE
            | NOWAIT
            | NTH_VALUE_P
            | NULL_P
            | NULLCOLS
            | NULLIF
            | NULLS_P
            | NUMBER_P
            | NUMERIC
            | NUMSTR
            | NVARCHAR
            | NVARCHAR2
            | NVL
            | OBJECT_P
            | OF
            | OFF
            | OIDS
            | ONLY
            | OPERATOR
            | OPTIMIZATION
            | OPTION
            | OPTIONALLY
            | OPTIONS
            | OR
            | OUT_P
            | OUTER_P
            | OUTFILE
            | OVERLAY
            | OWNED
            | OWNER
            | PACKAGE
            | PACKAGES
			| TSQL_PAGLOCK
            | PARALLEL_ENABLE
            | PARSER
            | PARTIAL
            | PARTITION
            | PARTITIONS
            | PASSING
            | PASSWORD
            | PCTFREE
            | PER_P
            | TSQL_PERCENT
            | PERFORMANCE
            | PERM
            | TSQL_PERSISTED
            | PIPELINED
            | PLACING
            | PLAN
            | PLANS
            | POLICY
            | POOL
            | POSITION
            | PRECEDES_P
            | PRECEDING
            | PREDICT
            | PREFERRED
            | PREFIX
            | PREPARE
            | PREPARED
            | PRESERVE
            | PRIMARY
            | PRIOR
            | PRIORER
            | PRIVATE
            | PRIVILEGE
            | PRIVILEGES
			| TSQL_PROC
            | PROCEDURAL
            | PROCEDURE
            | PROFILE
            | PUBLICATION
            | PUBLISH
            | PURGE
            | QUERY
            | QUOTE
            | RANDOMIZED
            | RANGE
            | RATIO
            | RAW
            | READ
			| TSQL_READCOMMITTED
			| TSQL_READPAST
			| TSQL_READUNCOMMITTED
            | REAL
            | REASSIGN
            | REBUILD
            | RECHECK
            | RECURSIVE
            | RECYCLEBIN
            | REDISANYVALUE
            | REF
            | REFERENCES
            | REFRESH
            | REINDEX
            | REJECT_P
            | RELATIVE_P
            | RELEASE
            | RELOPTIONS
            | REMOTE_P
            | REMOVE
            | RENAME
            | REPEAT
            | REPEATABLE
			| TSQL_REPEATABLEREAD
            | REPLACE
            | REPLICA
            | RESEED
            | RESET
            | RESIZE
            | RESOURCE
            | RESPECT_P
            | RESTART
            | RESTRICT
            | RESULT
            | RETURN
            | RETURNED_SQLSTATE
            | RETURNS
            | REUSE
            | REVOKE
            | RIGHT
            | ROLE
            | ROLES
            | ROLLBACK
            | ROLLUP
            | ROTATE
            | ROTATION
            | ROW
            | ROW_COUNT
			| TSQL_ROWLOCK
            | ROWNUM
            | ROWS
            | ROWTYPE_P
            | RULE
            | SAMPLE
			| SAVE
            | SAVEPOINT
            | SCHEDULE
            | SCHEMA
            | SCHEMA_NAME
            | SCROLL
            | SEARCH
            | SECURITY
            | SELF
            | SEPARATOR_P
            | SEQUENCE
            | SEQUENCES
            | SERIALIZABLE
            | SERVER
            | SESSION
            | SESSION_USER
            | SET
            | SETOF
            | SETS
            | SHARE
            | SHIPPABLE
            | SHOW
            | SHRINK
            | SHUTDOWN
            | SIBLINGS
            | SIMILAR
            | SIMPLE
            | SIZE
            | SKIP
            | SLAVE
            | SLICE
            | SMALLDATETIME
            | SMALLDATETIME_FORMAT_P
            | SMALLINT
            | SNAPSHOT
            | SOME
            | SOURCE_P
            | SPACE
            | SPECIFICATION
            | SPILL
            | SPLIT
            | SQL_P
            | STABLE
            | STACKED_P
            | STANDALONE_P
            | START
            | STARTING
            | STARTS
            | STATEMENT
            | STATEMENT_ID
            | STATIC_P
            | STATISTICS
            | STDIN
            | STDOUT
            | STORAGE
            | STORE_P
            | STORED
            | STRATIFY
            | STREAM
            | STRICT_P
            | STRIP_P
            | SUBCLASS_ORIGIN
            | SUBPARTITION
            | SUBPARTITIONS
            | SUBSCRIPTION
            | SUBSTRING
            | SYMMETRIC
            | SYNONYM
            | SYS_REFCURSOR
            | SYSDATE
            | SYSID
            | SYSTEM_P
            | TABLE
            | TABLE_NAME
            | TABLES
            | TABLESAMPLE
            | TABLESPACE
			| TSQL_TABLOCK
			| TSQL_TABLOCKX
            | TEMP
            | TEMPLATE
            | TEMPORARY
            | TERMINATED
            | TEXT_P
			| TSQL_TEXTIMAGE_ON
            | THAN
            | THEN
            | TIES
            | TIME
            | TIME_FORMAT_P
            | TIMECAPSULE
            | TIMESTAMP
            | TIMESTAMP_FORMAT_P
            | TIMESTAMPDIFF
            | TIMEZONE_HOUR_P
            | TIMEZONE_MINUTE_P
            | TINYINT
            | TSQL_TOP
			| TSQL_QUARTER
			| TSQL_YYYY
			| TSQL_YY
			| TSQL_Q
			| TSQL_QQ
			| TSQL_MM
			| TSQL_M
			| TSQL_DAYOFYEAR
			| TSQL_DY
			| TSQL_Y
			| TSQL_WEEK
			| TSQL_WK
			| TSQL_WW
			| TSQL_WEEKDAY
			| TSQL_DW
			| TSQL_W
			| TSQL_DD
			| TSQL_D
			| TSQL_HH
			| TSQL_MI
			| TSQL_N
			| TSQL_SS
			| TSQL_S
			| TSQL_MILLISECOND
			| TSQL_MS
			| TSQL_MICROSECOND
			| TSQL_MCS
			| TSQL_NANOSECOND
			| TSQL_NS
            | TRAILING
			| TRAN
            | TRANSACTION
            | TRANSFORM
            | TREAT
            | TRIGGER
            | TRIM
            | TRUE_P
            | TRUNCATE
            | TRUSTED
            | TSFIELD
            | TSTAG
            | TSTIME
            | TYPES_P
            | UNBOUNDED
            | UNCOMMITTED
            | UNDER
            | UNENCRYPTED
            | UNIMCSTORED
            | UNIQUE
            | UNKNOWN
            | UNLIMITED
            | UNLISTEN
            | UNLOCK
            | UNLOGGED
            | UNTIL
            | UNUSABLE
            | UPDATE
			| TSQL_UPDLOCK
            | USE_P
            | USEEOF
            | USER
            | USING
            | VACUUM
            | VALID
            | VALIDATE
            | VALIDATION
            | VALIDATOR
            | VALUES
            | VARCHAR
            | VARCHAR2
            | VARIABLES
            | VARIADIC
            | VARRAY
            | VCGROUP
            | VERBOSE
            | VERIFY
            | VERSION_P
            | VIEW
            | VISIBLE
            | VOLATILE
            | WAIT
            | WARNINGS
            | WEAK
            | WHEN
            | WHILE_P
            | WHITESPACE_P
            | WORK
            | WORKLOAD
            | WRAPPER
            | WRITE
			| TSQL_XLOCK
            | XMLATTRIBUTES
            | XMLCONCAT
            | XMLELEMENT
            | XMLEXISTS
            | XMLFOREST
            | XMLPARSE
            | XMLPI
            | XMLROOT
            | XMLSERIALIZE
            | YEAR_MONTH_P
            | YES_P
            | ZONE
		;

tsql_opt_transaction_name:
			ColId
			| /*EMPTY*/								{ $$ = NULL; }
		;

tsql_transaction_keywords:
			TRAN
			| TRANSACTION
		;

tsql_TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| START TRANSACTION WITH CONSISTENT SNAPSHOT
				{
					if (!DB_IS_CMPT(B_FORMAT)) {
						const char* message = "WITH CONSISTENT SNAPSHOT is supported only in B-format database.";
						InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
						ereport(errstate,
							(errmodule(MOD_PARSER),
								errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("WITH CONSISTENT SNAPSHOT is supported only in B-format database."),
								parser_errposition(@3)));
					}
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = NIL;
					n->with_snapshot = true;
					$$ = (Node *)n;
				}
			| BEGIN_NON_ANOYBLOCK transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $2;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3)));
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($5)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($4)));
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{   
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{   
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					n->csn = InvalidCommitSeqNo;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst WITH Sconst
				{   
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					n->csn = strtoull($5, NULL, 10);;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{   
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| BEGIN_NON_ANOYBLOCK tsql_transaction_keywords tsql_opt_transaction_name
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| COMMIT tsql_transaction_keywords ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| COMMIT TRAN
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK tsql_transaction_keywords ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					if ($3 == NULL) {
						n->kind = TRANS_STMT_ROLLBACK;
						n->options = NIL;
					} else {
						n->kind = TRANS_STMT_ROLLBACK_TO;
						n->options = list_make1(makeDefElem("savepoint_name",
															(Node *)makeString($3)));
					}
					$$ = (Node *)n;
				}
			| ROLLBACK TRAN
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVE tsql_transaction_keywords ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3)));
					$$ = (Node *)n;
				}
                       | BEGIN_NON_ANOYBLOCK TRY
                                {
                                        TransactionStmt *n = makeNode(TransactionStmt);
                                        n->kind = TRANS_STMT_BEGIN_TRY;
                                        n->options = NIL;
                                        $$ = (Node *)n;
                                }
                       | END_P TRY BEGIN_P CATCH
                                {
                                        TransactionStmt *n = makeNode(TransactionStmt);
                                        n->kind = TRANS_STMT_END_TRY_BEGIN_CATCH;
                                        n->options = NIL;
                                        $$ = (Node *)n;
                                }
                       | END_P CATCH
                                {
                                        TransactionStmt *n = makeNode(TransactionStmt);
                                        n->kind = TRANS_STMT_END_CATCH;
                                        n->options = NIL;
                                        $$ = (Node *)n;
                                }
		;

/*
 * SQL table hints apply to DELETE, INSERT, SELECT and UPDATE statements.
 * In SELECT statement, it's specified in the FROM clause.
 * Table hint can start without WITH keyword. To avoid s/r conflict, we handle
 * such cases by looking up an additional token and check if it's a valid hint,
 * and re-assign the token '(' to TSQL_HINT_START_BRACKET.
 * when used without "WITH", the table hint can only be specified alone.
 */
tsql_opt_table_hint_expr_with:
			tsql_table_hint_expr_with                       { $$ = $1; }
			| /*EMPTY*/                                     { $$ = NIL; }
		;

tsql_table_hint_expr_with:
			WITH_paren TSQL_HINT_START_BRACKET tsql_table_hint_list ')'       { $$ = $3; }
		;

tsql_table_hint_expr_no_with:
            TSQL_HINT_START_BRACKET tsql_table_hint ')' 			{ $$ = list_make1($2); }

tsql_table_hint_list:
			tsql_table_hint
				{
					$$ = list_make1($1);
				}
			| tsql_table_hint_list ',' tsql_table_hint
				{
					$$ = lappend($1, $3);
				}
			| tsql_table_hint_list tsql_table_hint
				{
					$$ = lappend($1, $2);
				}
		;

tsql_table_hint:
			tsql_table_hint_kw_no_with
				{
					$$ = (Node* ) $1;
					ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("The %s option is currently ignored", $1)));
				}
		;

/*
 * Table hints that can be used without "WITH" keyword.
 * We explicitly add these keywords only to allow us to detect
 * TSQL_HINT_START_BRACKET to avoid s/r conflicts. It seems unnecessary to
 * add all the hints since we do not need to do anything with them yet.
 * It is up to the designer of table hint later to decide whether we should
 * add all hints as keywords or just do some checking inside the code block.
 */
tsql_table_hint_kw_no_with:
			TSQL_NOLOCK                             {$$ = pstrdup($1);}
			| TSQL_READUNCOMMITTED                  {$$ = pstrdup($1);}
			| TSQL_UPDLOCK                          {$$ = pstrdup($1);}
			| TSQL_REPEATABLEREAD                   {$$ = pstrdup($1);}
			| SERIALIZABLE                          {$$ = pstrdup($1);}
			| TSQL_READCOMMITTED                    {$$ = pstrdup($1);}
			| TSQL_TABLOCK                          {$$ = pstrdup($1);}
			| TSQL_TABLOCKX                         {$$ = pstrdup($1);}
			| TSQL_PAGLOCK                          {$$ = pstrdup($1);}
			| TSQL_ROWLOCK                          {$$ = pstrdup($1);}
			| NOWAIT                                {$$ = pstrdup($1);}
			| TSQL_READPAST                         {$$ = pstrdup($1);}
			| TSQL_XLOCK                            {$$ = pstrdup($1);}
			| SNAPSHOT                              {$$ = pstrdup($1);}
			| TSQL_NOEXPAND                         {$$ = pstrdup($1);}
		;

tsql_InsertStmt: opt_with_clause INSERT hint_string INTO insert_target tsql_opt_table_hint_expr_with insert_rest returning_clause
			{
				$7->relation = $5;
				$7->returningList = $8;
				$7->withClause = $1;
				$7->isReplace = false;
				$7->hintState = create_hintstate($3);
				$7->hasIgnore = ($7->hintState != NULL && $7->hintState->sql_ignore_hint && DB_IS_CMPT(B_FORMAT));
				$$ = (Node *) $7;
			}
			| opt_with_clause INSERT hint_string insert_target tsql_opt_table_hint_expr_with insert_rest returning_clause
			{
				$6->relation = $4;
				$6->returningList = $7;
				$6->withClause = $1;
				$6->isReplace = false;
				$6->hintState = create_hintstate($3);
				$$ = (Node *) $6;
			}
			| opt_with_clause INSERT hint_string insert_target tsql_opt_table_hint_expr_with insert_rest upsert_clause returning_clause
				{
					if ($1 != NULL) {
						const char* message = "WITH clause is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.";
    					InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
						ereport(errstate,
							(errmodule(MOD_PARSER),
							 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("%s", message)));
					}

					if (u_sess->attr.attr_sql.enable_upsert_to_merge
#ifdef ENABLE_MULTIPLE_NODES					
					    ||t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM
#endif						
					    ) {

						if ($6 != NULL && $6->cols != NIL) {
							ListCell *c = NULL;
							List *cols = $6->cols;
							foreach (c, cols) {
								ResTarget *rt = (ResTarget *)lfirst(c);
								if (rt->indirection != NIL) {
									const char* message = "Try assign a composite or an array expression to column ";
    								InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
									ereport(errstate,
										(errmodule(MOD_PARSER),
										 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("Subfield name or array subscript of column \"%s\" "
											"is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.",
											rt->name),
										 errhint("%s\"%s\".", message, rt->name)));
								}
							}
						}


						MergeStmt *m = makeNode(MergeStmt);
						m->is_insert_update = true;

						/* for UPSERT, keep the INSERT statement as well */
						$6->relation = $4;
						$6->returningList = $8;
						$6->isReplace = false;
						$6->withClause = $1;
						$6->hintState = create_hintstate($3);
#ifdef ENABLE_MULTIPLE_NODES						
						if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
							UpsertClause *uc = makeNode(UpsertClause);
							if ($7 == NULL)
								uc->targetList = NIL;
							else
								uc->targetList = ((MergeWhenClause *)$7)->targetList;
							$6->upsertClause = uc;
						}
#endif						
						m->insert_stmt = (Node *)copyObject($6);

						/* fill a MERGE statement*/
						m->relation = $4;

						Alias *a1 = makeAlias(($4->relname), NIL);
						$4->alias = a1;

						Alias *a2 = makeAlias("excluded", NIL);
						RangeSubselect *r = makeNode(RangeSubselect);
						r->alias = a2;
						r->subquery = (Node *) ($6->selectStmt);
						m->source_relation = (Node *) r;

						MergeWhenClause *n = makeNode(MergeWhenClause);
						n->matched = false;
						n->commandType = CMD_INSERT;
						n->cols = $6->cols;
						n->values = NULL;

						m->mergeWhenClauses = list_make1((Node *) n);
						if ($7 != NULL)
							m->mergeWhenClauses = list_concat(list_make1($7), m->mergeWhenClauses);


						$$ = (Node *)m;
					} else {
						$6->relation = $4;
						$6->returningList = $8;
						$6->withClause = $1;
						$6->upsertClause = (UpsertClause *)$7;
						$6->isReplace = false;
						$6->hintState = create_hintstate($3);   
						$$ = (Node *) $6;
					}
				}
			| opt_with_clause INSERT hint_string INTO insert_target tsql_opt_table_hint_expr_with insert_rest upsert_clause returning_clause
				{
					if ($1 != NULL) {
						const char* message = "WITH clause is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.";
    					InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
						ereport(errstate,
							(errmodule(MOD_PARSER),
							 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH clause is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.")));
					}

					if (u_sess->attr.attr_sql.enable_upsert_to_merge
#ifdef ENABLE_MULTIPLE_NODES					
					    ||t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM
#endif						
					    ) {

						if ($7 != NULL && $7->cols != NIL) {
							ListCell *c = NULL;
							List *cols = $7->cols;
							foreach (c, cols) {
								ResTarget *rt = (ResTarget *)lfirst(c);
								if (rt->indirection != NIL) {
									const char* message = "Try assign a composite or an array expression to column ";
    								InsertErrorMessage(message, u_sess->plsql_cxt.plpgsql_yylloc);
									ereport(errstate,
										(errmodule(MOD_PARSER),
										 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("Subfield name or array subscript of column \"%s\" "
											"is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.",
											rt->name),
										 errhint("Try assign a composite or an array expression to column \"%s\".", rt->name)));
								}
							}
						}


						MergeStmt *m = makeNode(MergeStmt);
						m->is_insert_update = true;

						/* for UPSERT, keep the INSERT statement as well */
						$7->relation = $5;
						$7->returningList = $9;
						$7->isReplace = false;
						$7->withClause = $1;
						$7->hintState = create_hintstate($3);
						$7->hasIgnore = ($7->hintState != NULL && $7->hintState->sql_ignore_hint && DB_IS_CMPT(B_FORMAT));
#ifdef ENABLE_MULTIPLE_NODES						
						if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
							UpsertClause *uc = makeNode(UpsertClause);
							if ($8 == NULL)
								uc->targetList = NIL;
							else
								uc->targetList = ((MergeWhenClause *)$8)->targetList;
							$7->upsertClause = uc;
						}
#endif						
						m->insert_stmt = (Node *)copyObject($7);

						/* fill a MERGE statement*/
						m->relation = $5;

						Alias *a1 = makeAlias(($5->relname), NIL);
						$5->alias = a1;

						Alias *a2 = makeAlias("excluded", NIL);
						RangeSubselect *r = makeNode(RangeSubselect);
						r->alias = a2;
						r->subquery = (Node *) ($7->selectStmt);
						m->source_relation = (Node *) r;

						MergeWhenClause *n = makeNode(MergeWhenClause);
						n->matched = false;
						n->commandType = CMD_INSERT;
						n->cols = $7->cols;
						n->values = NULL;

						m->mergeWhenClauses = list_make1((Node *) n);
						if ($8 != NULL)
							m->mergeWhenClauses = list_concat(list_make1($7), m->mergeWhenClauses);
						$$ = (Node *)m;
					} else {
						$7->relation = $5;
						$7->returningList = $9;
						$7->withClause = $1;
						$7->upsertClause = (UpsertClause *)$8;
						$7->isReplace = false;
						$7->hintState = create_hintstate($3);   
						$7->hasIgnore = ($7->hintState != NULL && $7->hintState->sql_ignore_hint && DB_IS_CMPT(B_FORMAT));
						$$ = (Node *) $7;
					}
				}
		;

/* table hint for delete statement */
delete_relation_expr_opt_alias_with_hint: delete_relation_expr_opt_alias tsql_table_hint_expr_with { $$ = (RangeVar*)$1; }
			;

relation_expr_opt_alias_list: 
			delete_relation_expr_opt_alias_with_hint                                      { $$ = list_make1($1); }
			| relation_expr_opt_alias_list ',' delete_relation_expr_opt_alias_with_hint   { $$ = lappend($1, $3); }
		;

/* table hint for update and select statement */
table_ref_for_no_table_function:
			relation_expr tsql_table_hint_expr_with  %prec UMINUS
				{
					$$ = (Node *) $1;
				}
			| relation_expr alias_clause tsql_table_hint_expr_with
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			| relation_expr opt_alias_clause tablesample_clause tsql_table_hint_expr_with
				{
					RangeTableSample *n = (RangeTableSample *) $3;
					$1->alias = $2;
					n->relation = (Node *) $1;
					$$ = (Node *) n;
				}
		;

/* table hint for select statement, can use without with keyword for single hint */
table_ref:
			relation_expr tsql_table_hint_expr_no_with  %prec UMINUS
				{
					$$ = (Node *) $1;
				}
			| relation_expr alias_clause tsql_table_hint_expr_no_with
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			| relation_expr opt_alias_clause tablesample_clause tsql_table_hint_expr_no_with
				{
					RangeTableSample *n = (RangeTableSample *) $3;
					$1->alias = $2;
					n->relation = (Node *) $1;
					$$ = (Node *) n;
				}
		;

AlterProcedureStmt:
			ALTER TSQL_PROC function_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->isProcedure = true;
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
		;

RenameStmt:
				ALTER TSQL_PROC function_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

AlterObjectSchemaStmt:
				ALTER TSQL_PROC function_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

AlterOwnerStmt:
				ALTER TSQL_PROC function_with_argtypes OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newowner = $6;
					$$ = (Node *)n;
				}

CompileStmt:
				ALTER TSQL_PROC function_with_argtypes COMPILE
				{
					u_sess->plsql_cxt.during_compile = true;
					CompileStmt *n = makeNode(CompileStmt);
					if (enable_plpgsql_gsdependency_guc()) {
						n->objName = ((FuncWithArgs*)$3)->funcname;
						n->funcArgs = ((FuncWithArgs*)$3)->funcargs;
						n->compileItem = COMPILE_PROCEDURE;
					}
					$$ = (Node*)n;
				}
				| ALTER TSQL_PROC func_name_opt_arg COMPILE
				{
					u_sess->plsql_cxt.during_compile = true;
					CompileStmt *n = makeNode(CompileStmt);
					if (enable_plpgsql_gsdependency_guc()) {
						n->objName = $3;
						n->funcArgs = NULL;
						n->compileItem = COMPILE_PROCEDURE;
					}
					$$ = (Node*)n;
				}
		;

RemoveFuncStmt:
            DROP TSQL_PROC func_name func_args opt_drop_behavior
                {
                    DropStmt *n = makeNode(DropStmt);
                    n->removeType = OBJECT_FUNCTION;
                    n->objects = list_make1($3);
                    n->arguments = list_make1(extractArgTypes($4));
                    n->behavior = $5;
                    n->missing_ok = false;
                    n->concurrent = false;
                    n->isProcedure = true;
                    $$ = (Node *)n;
                }
            | DROP TSQL_PROC IF_P EXISTS func_name func_args opt_drop_behavior
                {
                    DropStmt *n = makeNode(DropStmt);
                    n->removeType = OBJECT_FUNCTION;
                    n->objects = list_make1($5);
                    n->arguments = list_make1(extractArgTypes($6));
                    n->behavior = $7;
                    n->missing_ok = true;
                    n->concurrent = false;
                    n->isProcedure = true;
                    $$ = (Node *)n;
                }
			| DROP TSQL_PROC func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($3);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = false;
					n->concurrent = false;
					n->isProcedure = true;
					$$ = (Node *)n;
				}
			| DROP TSQL_PROC IF_P EXISTS func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($5);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = true;
					n->concurrent = false;
					n->isProcedure = true;
					$$ = (Node *)n;
				}
		;

/*
 * NOTE: the OptFileGroup production doesn't really belong here. We accept OptFileGroup
 *       for TSQL compatibility, but that syntax is used to place a table on
 *       a filegroup (analogous to a tablespace).  For now, we just accept the
 *       filegroup specification and ignore it. This makes it impossible to
 *       write an ON COMMIT option and an ON filegroup clause in the same
 *       statement, but that would be illegal syntax anyway.
 */
OnCommitOption:
            FileGroup                    { $$ = ONCOMMIT_NOOP; }
			| TextFileGroup              { $$ = ONCOMMIT_NOOP; }
			| FileGroup TextFileGroup    { $$ = ONCOMMIT_NOOP; }

FileGroup:
            ON filegroupname {}
		;

TextFileGroup:
            TSQL_TEXTIMAGE_ON filegroupname {}
		;

filegroupname:
            file_group_name              
			| '[' file_group_name ']'
			| SCONST
		;

file_group_name:	IDENT
				{
					$$ = IdentResolveToChar($1, yyscanner);
				}
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
			| reserved_keyword                      { $$ = pstrdup($1); }
		;



OptFileGroup: FileGroup
              | /*EMPTY*/

tsql_UseStmt:
			USE_P ColId
				{
                    char *curDbName = NULL;
                    curDbName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
                    if (pg_strcasecmp(curDbName, $2) != 0) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    			errmsg("Use of non-current database '%s' is not supported.", $2)));
                    } else {
                        ereport(NOTICE, (errmsg("Already connected to database '%s'.", $2)));

                        // set a virtual value and nothing to do
                        VariableSetStmt *n = makeNode(VariableSetStmt);
                        n->kind = VAR_SET_VALUE;
                        n->name = "_d_virtual_value";
                        n->args = list_make1(makeStringConst("_d_virtual_value", -1));
                        n->is_local = false;
                        $$ = (Node *) n;
                    }
                }
            ;

def_elem:   ColLabel '=' ROW
				{
					$$ = makeDefElem($1, (Node *) makeString(pstrdup($3)));
				}
			| ColLabel '=' NONE
				{
					$$ = makeDefElem($1, (Node *) makeString(pstrdup($3)));
				}
			| tsql_with_compression_delay_minutes
			    {
                    $$ = $1;
			    }
		    ;

tsql_with_compression_delay_minutes: ColLabel '=' tsql_UnsignedNumericOnly tsql_minutes_options
			    {
					if (pg_strcasecmp($1, "compression_delay") == 0) {
						$$ = makeDefElem($1, (Node *)$3);
					} else {
						ereport(ERROR,
						    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						        errmsg("The %s option with minute unit is currently not supported", $1)));
					}
				}
			;
tsql_minutes_options:   MINUTE_P
                        | TSQL_MINUTES_P
		            ;

tsql_UnsignedNumericOnly:   Iconst								{ $$ = makeInteger($1); }
                            | FCONST                            { $$ = makeFloat($1); }



alter_table_cmd:
	TSQL_CONVERT TO convert_charset opt_collate
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_ConvertCharset;
				CharsetCollateOptions *cc = makeNode(CharsetCollateOptions);
				cc->cctype = OPT_CHARSETCOLLATE;
				cc->charset = $3;
				cc->collate = $4;
				n->def = (Node *)cc;
				$$ = (Node*)n;
			}
		;

/* DATEDIFF() arguments
 */
datediff_arg:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| TSQL_YYYY                             { $$ = "year"; }
			| TSQL_YY                               { $$ = "year"; }
			| TSQL_QUARTER                          { $$ = "quarter"; }
			| TSQL_Q                                { $$ = "quarter"; }
			| TSQL_QQ                               { $$ = "quarter"; }
			| MONTH_P								{ $$ = "month"; }
			| TSQL_MM                               { $$ = "month"; }
			| TSQL_M                                { $$ = "month"; }
			| TSQL_DAYOFYEAR                        { $$ = "doy"; }
			| TSQL_DY                               { $$ = "doy"; }
			| TSQL_Y                                { $$ = "doy"; }
			| TSQL_WEEK                             { $$ = "week"; }
			| TSQL_WK                               { $$ = "week"; }
			| TSQL_WW                               { $$ = "week"; }
			| TSQL_WEEKDAY                          { $$ = "weekday"; }
			| TSQL_DW                               { $$ = "weekday"; }
			| TSQL_W                                { $$ = "weekday"; }
			| DAY_P									{ $$ = "day"; }
			| TSQL_DD								{ $$ = "day"; }
			| TSQL_D								{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| TSQL_HH                               { $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| TSQL_MI                               { $$ = "minute"; }
			| TSQL_N                                { $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| TSQL_SS                               { $$ = "second"; }
			| TSQL_S                                { $$ = "second"; }
			| TSQL_MILLISECOND                      { $$ = "millisecond"; }
			| TSQL_MS                               { $$ = "millisecond"; }
			| TSQL_MICROSECOND                      { $$ = "microsecond"; }
			| TSQL_MCS                              { $$ = "microsecond"; }
			| TSQL_NANOSECOND                       { $$ = "nanosecond"; }
			| TSQL_NS                               { $$ = "nanosecond"; }
			| Sconst								{ $$ = $1; }
		;
