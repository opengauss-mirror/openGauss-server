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

unreserved_keyword:
			TSQL_CLUSTERED
			| TSQL_NONCLUSTERED ;

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
		;