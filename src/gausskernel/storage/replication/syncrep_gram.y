%{
/* -------------------------------------------------------------------------
 *
 * syncrep_gram.y				- Parser for synchronous_standby_names
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_gram.y
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "replication/syncrep_gramparse.h"

static SyncRepConfigData *create_syncrep_config(const char *num_sync,
								  List *members, uint8 syncrep_method);

#define parser_yyerror(msg)  syncrep_scanner_yyerror(msg, yyscanner)
static void syncrep_yyerror(YYLTYPE *yylloc,
							syncrep_scanner_yyscan_t yyscanner,
							const char *msg);

/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

%}

%define api.pure
%parse-param {syncrep_scanner_yyscan_t yyscanner}
%lex-param   {syncrep_scanner_yyscan_t yyscanner}
%locations

%expect 0
%name-prefix "syncrep_yy"


%union
{
	syncrep_scanner_YYSTYPE	yy_core;
	char	   *str;
	List	   *list;
	SyncRepConfigData *config;
}

%token <str> NAME NUM JUNK ANY FIRST

%type <config> standby_config_simplify standby_config_complete
%type <list> result standby_config standby_config_combination standby_list
%type <str> standby_name

%start result

%%
result:
		standby_config				{ t_thrd.syncrepgram_cxt.syncrep_parse_result = $1; }
	;

standby_config:
		standby_config_simplify						{ $$ = list_make1($1); }
		| standby_config_combination				{ $$ = $1; }
	;

standby_config_combination:
		standby_config_complete											{ $$ = list_make1($1);  }
		| standby_config_combination ',' standby_config_complete		{ $$ = lappend($1, $3); }
	;

standby_config_simplify:
		standby_list				{ $$ = create_syncrep_config("1", $1, SYNC_REP_PRIORITY); }
		| NUM '(' standby_list ')'	{ $$ = create_syncrep_config($1, $3, SYNC_REP_PRIORITY); pfree($1); }
	;

standby_config_complete:
		ANY NUM '(' standby_list ')'   { $$ = create_syncrep_config($2, $4, SYNC_REP_QUORUM); pfree($2); }
		| FIRST NUM '(' standby_list ')' { $$ = create_syncrep_config($2, $4, SYNC_REP_PRIORITY); pfree($2); }
	;

standby_list:
		standby_name						{ $$ = list_make1($1); }
		| standby_list ',' standby_name		{ $$ = lappend($1, $3); }
	;

standby_name:
		NAME						{ $$ = $1; }
		| NUM						{ $$ = $1; }
	;
%%

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
syncrep_yyerror(YYLTYPE *yylloc, syncrep_scanner_yyscan_t yyscanner,
									const char *msg)
{
	parser_yyerror(msg);
}

static SyncRepConfigData *
create_syncrep_config(const char *num_sync, List *members, uint8 syncrep_method)
{
	SyncRepConfigData *config;
	int			size;
	ListCell   *lc;
	char	   *ptr;
	int         used_size;
	int         name_size;
	errno_t rc = 0;

	/* Compute space needed for flat representation */
	size = offsetof(SyncRepConfigData, member_names);
	foreach(lc, members)
	{
		char	   *standby_name = (char *) lfirst(lc);

		size += strlen(standby_name) + 1;
	}

	/* And transform the data into flat representation */
	config = (SyncRepConfigData *) palloc(size);

	config->config_size = size;
	config->num_sync = atoi(num_sync);
	config->syncrep_method = syncrep_method;
	config->nmembers = list_length(members);
	ptr = config->member_names; 
	
	used_size = 0;
	name_size = config->config_size - offsetof(SyncRepConfigData, member_names);
	foreach(lc, members)
	{
		char	   *standby_name = (char *) lfirst(lc);

		rc = strncpy_s(ptr, (name_size - used_size), standby_name, strlen(standby_name));
		securec_check_c(rc, "\0", "\0");
		
		ptr += strlen(standby_name) + 1;
		used_size += strlen(standby_name) + 1;
	}
	lc = list_head(members);
	while (lc != NULL)
	{
		ListCell *tmp = lc;
		char *standbyName = (char *) lfirst(lc);
		lc = lnext(lc);
		/* we do not need free "*" because it is not from palloc */
		if (strcmp(standbyName, "*") != 0) {
			pfree(lfirst(tmp));
		}
		pfree(tmp);
	}
	if (members != NULL) {
		pfree(members);
	}

	return config;
}

#undef yyerror
#undef yylval
#undef yylloc
#undef yylex

#undef yylex
#include "syncrep_scanner.inc"
