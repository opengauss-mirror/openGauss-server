%{
/* -------------------------------------------------------------------------
 *
 * repl_gram.y				- Parser for the replication commands
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/replication/repl_gram.y
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogdefs.h"
#include "nodes/makefuncs.h"
#include "nodes/replnodes.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "replication/repl_gramparse.h"

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

#define parser_yyerror(msg)  replication_scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  replication_scanner_errposition(pos)

%}

%define api.pure
%parse-param {replication_scanner_yyscan_t yyscanner}
%lex-param   {replication_scanner_yyscan_t yyscanner}
%locations

%expect 0
%name-prefix "replication_yy"

%union {
		replication_scanner_YYSTYPE yy_core;
		char					*str;
		bool					boolval;
		int						ival;

		XLogRecPtr				recptr;
		Node					*node;
		List					*list;
		DefElem					*defelt;
}

/* Non-keyword tokens */
%token <str> SCONST IDENT
%token <recptr> RECPTR
%token <ival>	ICONST

/* Keyword tokens. */
%token K_BASE_BACKUP
%token K_IDENTIFY_SYSTEM
%token K_IDENTIFY_VERSION
%token K_IDENTIFY_MODE
%token K_IDENTIFY_MAXLSN
%token K_IDENTIFY_CONSISTENCE
%token K_IDENTIFY_CHANNEL
%token K_IDENTIFY_AZ
%token K_LABEL
%token K_PROGRESS
%token K_FAST
%token K_NOWAIT
%token K_BUILDSTANDBY
%token K_OBSMODE
%token K_COPYSECUREFILE
%token K_NEEDUPGRADEFILE
%token K_WAL
%token K_TABLESPACE_MAP
%token K_DATA
%token K_START_REPLICATION
%token K_FETCH_MOT_CHECKPOINT
%token K_ADVANCE_REPLICATION
%token K_CREATE_REPLICATION_SLOT
%token K_DROP_REPLICATION_SLOT
%token K_PHYSICAL
%token K_LOGICAL
%token K_SLOT

%type <node>	command
%type <node>	base_backup start_replication start_data_replication fetch_mot_checkpoint start_logical_replication advance_logical_replication identify_system identify_version identify_mode identify_consistence create_replication_slot drop_replication_slot identify_maxlsn identify_channel identify_az
%type <list>	base_backup_opt_list
%type <defelt>	base_backup_opt
%type <list>    plugin_options plugin_opt_list
%type <defelt>  plugin_opt_elem
%type <node>    plugin_opt_arg
%type <str>		opt_slot
%%

firstcmd: command opt_semicolon
				{
					t_thrd.replgram_cxt.replication_parse_result = $1;
				}
			;

opt_semicolon:	';'
				| /* EMPTY */
				;

command:
			identify_system
			| identify_version
			| identify_mode
			| identify_consistence
			| base_backup
			| start_replication
			| start_data_replication
			| fetch_mot_checkpoint
			| start_logical_replication
			| advance_logical_replication
			| create_replication_slot
			| drop_replication_slot
			| identify_maxlsn
			| identify_channel
			| identify_az
			;

/*
 * IDENTIFY_SYSTEM
 */
identify_system:
			K_IDENTIFY_SYSTEM
				{
					$$ = (Node *) makeNode(IdentifySystemCmd);
				}
			;

/*
 * IDENTIFY_VERSION
 */
identify_version:
			K_IDENTIFY_VERSION
				{
					$$ = (Node *) makeNode(IdentifyVersionCmd);
				}
			;

/*
 * IDENTIFY_MODE
 */
identify_mode:
			K_IDENTIFY_MODE
				{
					$$ = (Node *) makeNode(IdentifyModeCmd);
				}
			;

/*
 * IDENTIFY_MAXLSN
 */
identify_maxlsn:
			K_IDENTIFY_MAXLSN
				{
					$$ = (Node *) makeNode(IdentifyMaxLsnCmd);
				}
			;

/*
 * IDENTIFY_CONSISTENCE %X/%X
 */
identify_consistence:
			K_IDENTIFY_CONSISTENCE RECPTR
				{
					IdentifyConsistenceCmd *cmd;

					cmd = makeNode(IdentifyConsistenceCmd);
					cmd->recordptr = $2;

					$$ = (Node *) cmd;
				}
			;

/*
 * IDENTIFY_CHANNEL %d
 */
identify_channel:
			K_IDENTIFY_CHANNEL ICONST
				{
					IdentifyChannelCmd *cmd;

					cmd = makeNode(IdentifyChannelCmd);
					cmd->channel_identifier = $2;

					$$ = (Node *) cmd;
				}
			;

/*
 * IDENTIFY_AZ
 */
identify_az:
			K_IDENTIFY_AZ
				{
					$$ = (Node *) makeNode(IdentifyAZCmd);
				}
			;

/*
 * BASE_BACKUP [LABEL '<label>'] [PROGRESS] [FAST] [WAL] [NOWAIT] [BUILDSTANDBY] [OBSMODE] [COPYSECUREFILE] 
 * [COPYUPGRADEFILE] [TABLESPACE_MAP]
 */
base_backup:
			K_BASE_BACKUP base_backup_opt_list
				{
					BaseBackupCmd *cmd = (BaseBackupCmd *) makeNode(BaseBackupCmd);
					cmd->options = $2;
					$$ = (Node *) cmd;
				}
			;

base_backup_opt_list: base_backup_opt_list base_backup_opt { $$ = lappend($1, $2); }
			| /* EMPTY */			{ $$ = NIL; }

base_backup_opt:
			K_LABEL SCONST
				{
				  $$ = makeDefElem("label",
						   (Node *)makeString($2));
				}
			| K_PROGRESS
				{
				  $$ = makeDefElem("progress",
						   (Node *)makeInteger(TRUE));
				}
			| K_FAST
				{
				  $$ = makeDefElem("fast",
						   (Node *)makeInteger(TRUE));
				}
			| K_WAL
				{
				  $$ = makeDefElem("wal",
						   (Node *)makeInteger(TRUE));
				}
			| K_NOWAIT
				{
				  $$ = makeDefElem("nowait",
						   (Node *)makeInteger(TRUE));
				}
			| K_BUILDSTANDBY
				{
				  $$ = makeDefElem("buildstandby",
				  		   (Node *)makeInteger(TRUE));
				}
			| K_COPYSECUREFILE
                                {
                                  $$ = makeDefElem("copysecurefile",
                                                   (Node *)makeInteger(TRUE));
                                }
                        | K_NEEDUPGRADEFILE
                                {
                                  $$ = makeDefElem("needupgradefile",
                                                   (Node *)makeInteger(TRUE));
                                }
                        | K_OBSMODE
                                {
                                  $$ = makeDefElem("obsmode",
                                                   (Node *)makeInteger(TRUE));
                                }
			| K_TABLESPACE_MAP
				{
			          $$ = makeDefElem("tablespace_map",
				                   (Node *)makeInteger(TRUE));
				}
			;

/*
 * START_REPLICATION %X/%X
 * START_REPLICATION [SLOT slot] [PHYSICAL] %X/%X
 */
start_replication:
			K_START_REPLICATION opt_slot opt_physical RECPTR
				{
					StartReplicationCmd *cmd;

					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_PHYSICAL;
 					cmd->slotname = $2;
 					cmd->startpoint = $4;

					$$ = (Node *) cmd;
				}
			;
			
/*
 * START_REPLICATION DATA
 */
start_data_replication:
			K_START_REPLICATION K_DATA
				{
					$$ = (Node *) makeNode(StartDataReplicationCmd);
				}
			;

/*
 * FETCH_MOT_CHECKPOINT
 */
fetch_mot_checkpoint:
                        K_FETCH_MOT_CHECKPOINT
                                {
                                        $$ = (Node *) makeNode(FetchMotCheckpointCmd);
                                }
                        ;

/* START_REPLICATION SLOT slot LOGICAL %X/%X options */
start_logical_replication:
            K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options
				{
					StartReplicationCmd *cmd;
					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;;
					cmd->slotname = $3;
					cmd->startpoint = $5;
					cmd->options = $6;
					$$ = (Node *) cmd;
				}
			;
	
/* ADVANCE_REPLICATION SLOT slot LOGICAL %X/%X %X/%X */
advance_logical_replication:
            K_ADVANCE_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR RECPTR
				{
					AdvanceReplicationCmd *cmd;
					cmd = makeNode(AdvanceReplicationCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;;
					cmd->slotname = $3;
					cmd->restart_lsn = $5;
					cmd->confirmed_flush = $6;
					$$ = (Node *) cmd;
				}
			;

/* CREATE_REPLICATION_SLOT SLOT slot [%X/%X] */
 create_replication_slot:
			/* CREATE_REPLICATION_SLOT SLOT slot PHYSICAL [init_slot_lsn] */
 			K_CREATE_REPLICATION_SLOT IDENT K_PHYSICAL RECPTR
 				{
 					CreateReplicationSlotCmd *cmd;
 					cmd = makeNode(CreateReplicationSlotCmd);
 					cmd->kind = REPLICATION_KIND_PHYSICAL;
 					cmd->slotname = $2;
 					cmd->init_slot_lsn = $4;
 					$$ = (Node *) cmd;
 				}
			/* CREATE_REPLICATION_SLOT slot LOGICAL plugin */
			| K_CREATE_REPLICATION_SLOT IDENT K_LOGICAL IDENT
 				{
					CreateReplicationSlotCmd *cmd;
					cmd = makeNode(CreateReplicationSlotCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;
					cmd->slotname = $2;
					cmd->plugin = $4;
					$$ = (Node *) cmd;
 				}
 			;
 
 /* DROP_REPLICATION_SLOT slot */
 drop_replication_slot:
 			K_DROP_REPLICATION_SLOT IDENT
 				{
 					DropReplicationSlotCmd *cmd;
 					cmd = makeNode(DropReplicationSlotCmd);
 					cmd->slotname = $2;
 					$$ = (Node *) cmd;
 				}
 			;


opt_physical :	K_PHYSICAL | /* EMPTY */;
 
 
opt_slot :	K_SLOT IDENT
 				{
 					$$ = $2;
 				}
 				| /* nothing */			{ $$ = NULL; }		

plugin_options:
			'(' plugin_opt_list ')'         { $$ = $2; }
				| /* EMPTY */                   { $$ = NIL; }
				;

plugin_opt_list:
			plugin_opt_elem
				{
					$$ = list_make1($1);
				}
			| plugin_opt_list ',' plugin_opt_elem
				{
					$$ = lappend($1, $3);
				}
			;
		
plugin_opt_elem:
			IDENT plugin_opt_arg
				{
					$$ = makeDefElem($1, $2);
				}
			;
			
plugin_opt_arg:
			SCONST                          { $$ = (Node *) makeString($1); }
			| /* EMPTY */                   { $$ = NULL; }
			;
%%

void
replication_yyerror(YYLTYPE *yylloc,
							replication_scanner_yyscan_t yyscanner,
							const char *msg)
{
	parser_yyerror(msg);
}

int
replication_yylex(YYSTYPE *lvalp, YYLTYPE *llocp,
							replication_scanner_yyscan_t yyscanner)
{
	return replication_scanner_yylex(&(lvalp->yy_core), llocp, yyscanner);
}

#undef yylex

#include "repl_scanner.inc"
