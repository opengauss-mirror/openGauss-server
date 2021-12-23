%{
/* -------------------------------------------------------------------------
 *
 * specparse.y
 *	  bison grammar for the isolation test file format
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "isolationtester.h"


TestSpec		parseresult;			/* result of parsing is left here */

%}

%expect 0
%name-prefix "spec_yy"

%union
{
	char	   *str;
	Session	   *session;
	Step	   *step;
	Permutation *permutation;
	struct
	{
		void  **elements;
		int		nelements;
	}			ptr_list;
}

%type <ptr_list> setup_list teardown_list
%type <str>  opt_setup
%type <str> setup teardown
%type <ptr_list> step_list session_list permutation_list opt_permutation_list
%type <ptr_list> string_list
%type <session> session
%type <step> step
%type <permutation> permutation

%token <str> sqlblock string
%token PERMUTATION SESSION SETUP STEP TEARDOWN TEST

%%

TestSpec:
			setup_list
			teardown_list
			session_list
			opt_permutation_list
			{
				parseresult.setupsqls = (char **) $1.elements;
				parseresult.nsetupsqls = $1.nelements;
				parseresult.teardownsqls = (char **) $2.elements;
				parseresult.nteardownsqls = $2.nelements;
				parseresult.sessions = (Session **) $3.elements;
				parseresult.nsessions = $3.nelements;
				parseresult.permutations = (Permutation **) $4.elements;
				parseresult.npermutations = $4.nelements;
			}
		;

setup_list:
			/* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}
			| setup_list setup
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
		;

opt_setup:
			/* EMPTY */			{ $$ = NULL; }
			| setup				{ $$ = $1; }
		;

setup:
			SETUP sqlblock		{ $$ = $2; }
		;

teardown_list:
			/* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}
			| teardown_list teardown
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
		;

teardown:
			TEARDOWN sqlblock	{ $$ = $2; }
		;

session_list:
			session_list session
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| session
			{
				$$.nelements = 1;
				$$.elements = (void**)malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

session:
			SESSION string setup_list step_list teardown_list
			{
				$$ = (Session*)malloc(sizeof(Session));
				$$->name = $2;
				$$->setupsql = (char **) $3.elements;
				$$->nsetupsql = $3.nelements;
				$$->steps = (Step **) $4.elements;
				$$->nsteps = $4.nelements;
				$$->teardownsql = (char **) $5.elements;
				$$->nteardownsql = $5.nelements;
			}
		;

step_list:
			step_list step
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| step
			{
				$$.nelements = 1;
				$$.elements = (void**)malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


step:
			STEP string sqlblock
			{
				$$ = (Step*)malloc(sizeof(Step));
				$$->name = $2;
				$$->sql = $3;
				$$->errormsg = NULL;
			}
		;


opt_permutation_list:
			permutation_list
			{
				$$ = $1;
			}
			| /* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}

permutation_list:
			permutation_list permutation
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| permutation
			{
				$$.nelements = 1;
				$$.elements = (void**)malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


permutation:
			PERMUTATION string_list
			{
				$$ = (Permutation*)malloc(sizeof(Permutation));
				$$->stepnames = (char **) $2.elements;
				$$->nsteps = $2.nelements;
			}
		;

string_list:
			string_list string
			{
				$$.elements = (void**)realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| string
			{
				$$.nelements = 1;
				$$.elements = (void**)malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

%%

#include "specscanner.cpp"
