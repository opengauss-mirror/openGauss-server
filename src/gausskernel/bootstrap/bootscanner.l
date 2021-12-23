%{
/* -------------------------------------------------------------------------
 *
 * bootscanner.l
 *	  a lexical scanner for the bootstrap parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/bootstrap/bootscanner.l
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/attnum.h"
#include "access/htup.h"
#include "access/itup.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "bootstrap/bootstrap.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/scansup.h"
#include "rewrite/prs2lock.h"
#include "storage/buf/block.h"
#include "storage/smgr/fd.h"
#include "storage/item/itemptr.h"
#include "storage/off.h"
#include "utils/rel.h"

/* Not needed now that this file is compiled as part of bootparse. */


/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),errmsg_internal("%s", msg)))

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="boot_yy"


D		[0-9]
oct		\\{D}{D}{D}
Exp		[Ee][-+]?{D}+
id		([A-Za-z0-9_]|{oct}|\-)+
sid		\"([^\"])*\"
arrayid [A-Za-z0-9_]+\[{D}*\]

%%

asc			{ return(ASC); }

desc			{ return(DESC); }

open			{ return(OPEN); }

close			{ return(XCLOSE); }

create			{ return(XCREATE); }

OID				{ return(OBJ_ID); }
bootstrap		{ return(XBOOTSTRAP); }
"shared_relation"	{ return(XSHARED_RELATION); }
"without_oids"	{ return(XWITHOUT_OIDS); }
"rowtype_oid"	{ return(XROWTYPE_OID); }
_null_			{ return(NULLVAL); }

insert			{ return(INSERT_TUPLE); }

","				{ return(COMMA); }
"="				{ return(EQUALS); }
"("				{ return(LPAREN); }
")"				{ return(RPAREN); }

[\n]			{ t_thrd.bootstrap_cxt.yyline++; }
[\t]			;
" "				;

^\#[^\n]* ; /* drop everything after "#" for comments */


"declare"		{ return(XDECLARE); }
"build"			{ return(XBUILD); }
"indices"		{ return(INDICES); }
"unique"		{ return(UNIQUE); }
"index"			{ return(INDEX); }
"on"			{ return(ON); }
"using"			{ return(USING); }
"toast"			{ return(XTOAST); }

{arrayid}		{
					yylval.str = pstrdup(MapArrayTypeName(yytext));
					return(ID);
				}
{id}			{
					yylval.str = scanstr(yytext);
					return(ID);
				}
{sid}			{
					yytext[strlen(yytext)-1] = '\0'; /* strip off quotes */
					yylval.str = scanstr(yytext+1);
					yytext[strlen(yytext)] = '"'; /* restore quotes */
					return(ID);
				}

(-)?{D}+"."{D}*({Exp})? |
(-)?{D}*"."{D}+({Exp})? |
(-)?{D}+{Exp}			{
							yylval.str = pstrdup(yytext);
							return(CONST_P);
						}

.				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("syntax error at line %d: unexpected character \"%s\"",
									t_thrd.bootstrap_cxt.yyline, yytext)));
				}



%%

void
yyerror(const char *message)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("%s at line %d",
					message, t_thrd.bootstrap_cxt.yyline)));
}
