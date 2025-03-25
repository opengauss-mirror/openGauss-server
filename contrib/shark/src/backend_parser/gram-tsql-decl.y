%type <list> tsql_stmtmulti
%type <node> DBCCCheckIdentStmt DBCCStmt tsql_stmt tsql_CreateProcedureStmt tsql_IndexStmt

%token <keyword> CHECKIDENT  DBCC  NO_INFOMSGS NORESEED RESEED TSQL_CLUSTERED TSQL_NONCLUSTERED TSQL_COLUMNSTORE TSQL_PERSISTED TSQL_TOP TSQL_PERCENT
%type <keyword>  tsql_opt_clustered tsql_opt_columnstore
%token <str>	TSQL_ATAT_IDENT
%type <boolean> opt_with_no_infomsgs
%type <node> TSQL_computed_column
%type <node> tsql_top_clause tsql_select_top_value
%type <boolean> tsql_opt_ties tsql_opt_percent
%type <str> DirectColLabel
%type <keyword> direct_label_keyword
