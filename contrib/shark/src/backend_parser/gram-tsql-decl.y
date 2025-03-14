%type <list> tsql_stmtmulti
%type <node> DBCCCheckIdentStmt DBCCStmt tsql_stmt tsql_CreateProcedureStmt tsql_IndexStmt

%token <keyword> CHECKIDENT  DBCC  NO_INFOMSGS NORESEED RESEED TSQL_CLUSTERED TSQL_NONCLUSTERED TSQL_COLUMNSTORE
%type <keyword>  tsql_opt_clustered tsql_opt_columnstore
%token <str>	TSQL_ATAT_IDENT
%type <boolean> opt_with_no_infomsgs
