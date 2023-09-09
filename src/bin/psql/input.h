/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/input.h
 */
#ifndef INPUT_H
#define INPUT_H

/*
 * If some other file needs to have access to readline/history, include this
 * file and save yourself all this work.
 *
 * USE_READLINE is the definite pointers regarding existence or not.
 */
#define USE_READLINE 1

#ifdef HAVE_LIBREADLINE

#if defined(HAVE_READLINE_READLINE_H)
#include <readline/readline.h>
#include <readline/history.h>
#elif defined(HAVE_EDITLINE_READLINE_H)
#include <editline/readline.h>
#elif defined(HAVE_READLINE_H)
#include <readline.h>
#endif   /* HAVE_READLINE_READLINE_H, etc */
#else
#include <editline/readline.h>
#endif   /* HAVE_LIBREADLINE */

#include "libpq/pqexpbuffer.h"

char* gets_interactive(const char* prompt, PQExpBuffer query_buf);
char* gets_fromFile(FILE* source);

void pg_append_history(const char* s, PQExpBuffer history_buf);
void pg_send_history(PQExpBuffer history_buf);
void setHistSize(const char* targetName, const char* targetValue, bool setToDefault);
extern bool useReadline;
extern bool SensitiveStrCheck(const char* target);

void initializeInput(int flags);

#endif /* INPUT_H */
