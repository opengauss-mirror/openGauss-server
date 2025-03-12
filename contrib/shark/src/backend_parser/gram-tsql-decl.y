%type <list> tsql_stmtmulti
%type <node> tsql_stmt tsql_CreateProcedureStmt tsql_IndexStmt

%token <keyword> TSQL_CLUSTERED TSQL_NONCLUSTERED TSQL_COLUMNSTORE
%type <keyword>  tsql_opt_clustered tsql_opt_columnstore
%token <str>	TSQL_ATAT_IDENT
