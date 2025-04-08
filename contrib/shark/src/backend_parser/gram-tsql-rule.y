/*
 *	The target production for the whole parse.
 */
stmtblock:	DIALECT_TSQL tsql_stmtmulti
			{
				pg_yyget_extra(yyscanner)->parsetree = $2;
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

tsql_IndexStmt:
				CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
				ON qualified_name access_method_clause '(' index_params ')'
				opt_include opt_reloptions OptPartitionElement opt_table_index_options where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $6;
					n->missing_ok = false;
					n->schemaname = $7->schemaname;
					n->idxname = $7->relname;
					n->relation = $9;
					n->accessMethod = $10;
					n->indexParams = $12;
					n->indexIncludingParams = $14;
					n->options = $15;
					n->tableSpace = $16;
					n->indexOptions = $17;
					n->whereClause = $18;
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
				| CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_table_index_options
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $6;
					n->missing_ok = false;
					n->schemaname = $7->schemaname;
					n->idxname = $7->relname;
					n->relation = $9;
					n->accessMethod = $10;
					n->indexParams = $12;
					n->partClause  = $15;
					n->indexIncludingParams = $16;
					n->options = $17;
					n->tableSpace = $18;
					n->indexOptions = $19;
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
				| CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					GLOBAL opt_include opt_reloptions OptTableSpace opt_table_index_options
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $6;
					n->missing_ok = false;
					n->schemaname = $7->schemaname;
					n->idxname = $7->relname;
					n->relation = $9;
					n->accessMethod = $10;
					n->indexParams = $12;
					n->partClause  = NULL;
					n->indexIncludingParams = $15;
					n->options = $16;
					n->tableSpace = $17;
					n->indexOptions = $18;
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
				| CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					opt_include opt_reloptions OptPartitionElement opt_index_options where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $6;
					n->missing_ok = true;
					n->schemaname = $10->schemaname;
					n->idxname = $10->relname;
					n->relation = $12;
					n->accessMethod = $13;
					n->indexParams = $15;
					n->indexIncludingParams = $17;
					n->options = $18;
					n->tableSpace = $19;
					n->indexOptions = $20;
					n->whereClause = $21;
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
				| CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_index_options
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $6;
					n->missing_ok = true;
					n->schemaname = $10->schemaname;
					n->idxname = $10->relname;
					n->relation = $12;
					n->accessMethod = $13;
					n->indexParams = $15;
					n->partClause  = $18;
					n->indexIncludingParams = $19;
					n->options = $20;
					n->tableSpace = $21;
					n->indexOptions = $22;
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
				| CREATE opt_unique tsql_opt_clustered tsql_opt_columnstore INDEX opt_concurrently IF_P NOT EXISTS opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					GLOBAL opt_include opt_reloptions OptTableSpace opt_index_options
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->missing_ok = true;
					n->unique = $2;
					n->concurrent = $6;
					n->schemaname = $10->schemaname;
					n->idxname = $10->relname;
					n->relation = $12;
					n->accessMethod = $13;
					n->indexParams = $15;
					n->partClause  = NULL;
					n->indexIncludingParams = $18;
					n->options = $19;
					n->tableSpace = $20;
					n->indexOptions = $21;
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

InsertStmt: opt_with_clause INSERT hint_string insert_target insert_rest returning_clause
			{
				$5->relation = $4;
				$5->returningList = $6;
				$5->withClause = $1;
				$5->isReplace = false;
				$5->hintState = create_hintstate($3);
				$$ = (Node *) $5;
			}
			| opt_with_clause INSERT hint_string insert_target insert_rest upsert_clause returning_clause
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

						if ($5 != NULL && $5->cols != NIL) {
							ListCell *c = NULL;
							List *cols = $5->cols;
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
						$5->relation = $4;
						$5->returningList = $7;
						$5->isReplace = false;
						$5->withClause = $1;
						$5->hintState = create_hintstate($3);
#ifdef ENABLE_MULTIPLE_NODES						
						if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
							UpsertClause *uc = makeNode(UpsertClause);
							if ($6 == NULL)
								uc->targetList = NIL;
							else
								uc->targetList = ((MergeWhenClause *)$6)->targetList;
							$5->upsertClause = uc;
						}
#endif						
						m->insert_stmt = (Node *)copyObject($5);

						/* fill a MERGE statement*/
						m->relation = $4;

						Alias *a1 = makeAlias(($4->relname), NIL);
						$4->alias = a1;

						Alias *a2 = makeAlias("excluded", NIL);
						RangeSubselect *r = makeNode(RangeSubselect);
						r->alias = a2;
						r->subquery = (Node *) ($5->selectStmt);
						m->source_relation = (Node *) r;

						MergeWhenClause *n = makeNode(MergeWhenClause);
						n->matched = false;
						n->commandType = CMD_INSERT;
						n->cols = $5->cols;
						n->values = NULL;

						m->mergeWhenClauses = list_make1((Node *) n);
						if ($6 != NULL)
							m->mergeWhenClauses = list_concat(list_make1($6), m->mergeWhenClauses);


						$$ = (Node *)m;
					} else {
						$5->relation = $4;
						$5->returningList = $7;
						$5->withClause = $1;
						$5->upsertClause = (UpsertClause *)$6;
						$5->isReplace = false;
						$5->hintState = create_hintstate($3);   
						$$ = (Node *) $5;
					}
				}

tsql_CreateProcedureStmt:
			CREATE opt_or_replace definer_user PROCEDURE func_name_opt_arg proc_args
			opt_createproc_opt_list as_is {
				u_sess->parser_cxt.eaten_declare = false;
				u_sess->parser_cxt.eaten_begin = false;
				pg_yyget_extra(yyscanner)->core_yy_extra.include_ora_comment = true;
                u_sess->parser_cxt.isCreateFuncOrProc = true;
				if (set_is_create_plsql_type()) {
					set_create_plsql_type_start();
					set_function_style_a();
				}
			} subprogram_body
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
			| TSQL_CLUSTERED
			| TSQL_NONCLUSTERED
			| TSQL_PERSISTED ;


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
		| DBCC CHECKIDENT '(' ColId_or_Sconst ',' RESEED ',' SignedIconst ')' opt_with_no_infomsgs
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->distinctClause = NIL;
				n->targetList = make_reseed_func(quote_identifier_wrapper($4, yyscanner), (Node*)makeIntConst($8, @8), $10);
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
			| AnonyBlockStmt
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
			| CreateFunctionStmt
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
			| DoStmt
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
			| InsertStmt
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
			| TransactionStmt
			| TruncateStmt
			| UnlistenStmt
			| UpdateStmt
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
            | CONVERT_P
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
            | ROWNUM
            | ROWS
            | ROWTYPE_P
            | RULE
            | SAMPLE
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
            | TEMP
            | TEMPLATE
            | TEMPORARY
            | TERMINATED
            | TEXT_P
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
            | TRAILING
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
