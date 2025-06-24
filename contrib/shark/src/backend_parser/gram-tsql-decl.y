%type <list> tsql_stmtmulti

%type <list> identity_seed_increment
%type <node> DBCCCheckIdentStmt DBCCStmt tsql_stmt tsql_CreateProcedureStmt tsql_IndexStmt tsql_TransactionStmt tsql_InsertStmt

%token <keyword> CHECKIDENT DBCC NO_INFOMSGS NORESEED RESEED SAVE TRAN TSQL_CLUSTERED TSQL_NONCLUSTERED TSQL_COLUMNSTORE TSQL_PERSISTED TSQL_TOP TSQL_PERCENT
%type <keyword>  tsql_opt_clustered tsql_opt_columnstore tsql_unique_clustered tsql_primary_key_clustered
%token <keyword> TSQL_NOLOCK TSQL_READUNCOMMITTED TSQL_UPDLOCK TSQL_REPEATABLEREAD TSQL_READCOMMITTED TSQL_TABLOCK TSQL_TABLOCKX TSQL_PAGLOCK TSQL_ROWLOCK TSQL_READPAST TSQL_XLOCK TSQL_NOEXPAND
%token <str>	TSQL_ATAT_IDENT
%type <boolean> opt_with_no_infomsgs tsql_opt_unique_clustered
%type <node> TSQL_computed_column  TSQL_AnonyBlockStmt TSQL_CreateFunctionStmt TSQL_DoStmt
%type <node> tsql_top_clause tsql_select_top_value
%type <boolean> tsql_opt_ties tsql_opt_percent
%type <str> DirectColLabel tsql_opt_transaction_name
%type <keyword> direct_label_keyword tsql_transaction_keywords
%token TSQL_UNIQUE_CLUSTERED TSQL_UNIQUE_NONCLUSTERED TSQL_PRIMAY_KEY_NONCLUSTERED TSQL_PRIMAY_KEY_CLUSTERED
%type <str> tsql_table_hint_kw_no_with
%type <list> tsql_table_hint_expr_no_with tsql_table_hint_expr_with tsql_table_hint_list tsql_opt_table_hint_expr_with
%type <node> tsql_table_hint
%type <range> delete_relation_expr_opt_alias_with_hint
/*
 * WITH_paren and TSQL_HINT_START_BRACKET are added to support table hints syntax WITH (<table_hint> [[,]...n]),
 * otherwise the parser cannot tell between 'WITH' and 'WITH (' and thus
 * lead to a shift/reduce conflict.
 */
%token WITH_paren TSQL_HINT_START_BRACKET
