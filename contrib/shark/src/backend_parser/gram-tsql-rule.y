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
		;

IndexStmt:
			CREATE opt_unique tsql_opt_columnstore
			INDEX opt_concurrently opt_index_name
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
		;

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
			| IndexStmt
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