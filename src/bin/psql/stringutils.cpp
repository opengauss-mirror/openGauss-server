/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/stringutils.c
 */
#include "common.h"
#include "postgres_fe.h"
#include <ctype.h>
#include "stringutils.h"

static void strip_quotes(char* source, char quote, char escape, int encoding);

/*
 * Replacement for strtok() (a.k.a. poor man's flex)
 *
 * Splits a string into tokens, returning one token per call, then NULL
 * when no more tokens exist in the given string.
 *
 * The calling convention is similar to that of strtok, but with more
 * frammishes.
 *
 * s -			string to parse, if NULL continue parsing the last string
 * whitespace - set of whitespace characters that separate tokens
 * delim -		set of non-whitespace separator characters (or NULL)
 * quote -		set of characters that can quote a token (NULL if none)
 * escape -		character that can quote quotes (0 if none)
 * e_strings -	if TRUE, treat E'...' syntax as a valid token
 * del_quotes - if TRUE, strip quotes from the returned token, else return
 *				it exactly as found in the string
 * encoding -	the active character-set encoding
 *
 * Characters in 'delim', if any, will be returned as single-character
 * tokens unless part of a quoted token.
 *
 * Double occurrences of the quoting character are always taken to represent
 * a single quote character in the data.  If escape isn't 0, then escape
 * followed by anything (except \0) is a data character too.
 *
 * The combination of e_strings and del_quotes both TRUE is not currently
 * handled.  This could be fixed but it's not needed anywhere at the moment.
 *
 * Note that the string s is _not_ overwritten in this implementation.
 *
 * NB: it's okay to vary delim, quote, and escape from one call to the
 * next on a single source string, but changing whitespace is a bad idea
 * since you might lose data.
 */
char* strtokx(const char* s, const char* whitespace, const char* delim, const char* quote, char escape, bool e_strings,
    bool del_quotes, int encoding)
{
    /* store the local copy of the users string
    * here */
    static char* storage = NULL; 
    /* pointer into storage where to continue on
    * next call */
    static char* string = NULL;  

    /* variously abused variables: */
    unsigned int offset;
    char* start = NULL;
    char* p = NULL;
    errno_t rc = EOK;

    if (s != NULL) {
        free(storage);
        storage = NULL;
        string = NULL;

        /*
         * We may need extra space to insert delimiter nulls for adjacent
         * tokens.	2X the space is a gross overestimate, but it's unlikely
         * that this code will be used on huge strings anyway.
         */
        storage = (char*)pg_malloc(2 * strlen(s) + 1);
        check_strcpy_s(strcpy_s(storage, 2 * strlen(s) + 1, s));
        string = storage;
    }

    if (storage == NULL)
        return NULL;

    /* skip leading whitespace */
    offset = (unsigned int)strspn(string, whitespace);
    start = &string[offset];

    /* end of string reached? */
    if (*start == '\0') {
        /* technically we don't need to free here, but we're nice */
        free(storage);
        storage = NULL;
        string = NULL;
        return NULL;
    }

    /* test if delimiter character */
    if ((delim != NULL) && (strchr(delim, *start) != NULL)) {
        /*
         * If not at end of string, we need to insert a null to terminate the
         * returned token.	We can just overwrite the next character if it
         * happens to be in the whitespace set ... otherwise move over the
         * rest of the string to make room.  (This is why we allocated extra
         * space above).
         */
        p = start + 1;
        if (*p != '\0') {
            if (strchr(whitespace, *p) == NULL) {
                rc = memmove_s(p + 1, strlen(p) + 1, p, strlen(p) + 1);
                securec_check_c(rc, "\0", "\0");
            }

            *p = '\0';
            string = p + 1;
        } else {
            /* at end of string, so no extra work */
            string = p;
        }

        return start;
    }

    /* check for E string */
    p = start;
    if (e_strings && (*p == 'E' || *p == 'e') && p[1] == '\'') {
        quote = "'";
        escape = '\\'; /* if std strings before, not any more */
        p++;
    }

    /* test if quoting character */
    if ((quote != NULL) && (strchr(quote, *p) != NULL)) {
        /* okay, we have a quoted token, now scan for the closer */
        char thisquote = *p++;

        for (; *p; p += PQmblen(p, encoding)) {
            if (*p == escape && p[1] != '\0') {
                p++; /* process escaped anything */
            } else if (*p == thisquote && p[1] == thisquote) {
                p++; /* process doubled quote */
            }
            else if (*p == thisquote) {
                p++; /* skip trailing quote */
                break;
            }
        }

        /*
         * If not at end of string, we need to insert a null to terminate the
         * returned token.	See notes above.
         */
        if (*p != '\0') {
            if (strchr(whitespace, *p) == NULL) {
                rc = memmove_s(p + 1, strlen(p) + 1, p, strlen(p) + 1);
                securec_check_c(rc, "\0", "\0");
            }
            *p = '\0';
            string = p + 1;
        } else {
            /* at end of string, so no extra work */
            string = p;
        }

        /* Clean up the token if caller wants that */
        if (del_quotes) {
            strip_quotes(start, thisquote, escape, encoding);
        }
        return start;
    }

    /*
     * Otherwise no quoting character.	Scan till next whitespace, delimiter
     * or quote.  NB: at this point, *start is known not to be '\0',
     * whitespace, delim, or quote, so we will consume at least one character.
     */
    offset = (unsigned int)strcspn(start, whitespace);

    if (delim != NULL) {
        unsigned int offset2 = (unsigned int)strcspn(start, delim);
        if (offset > offset2) {
            offset = offset2;
        }
    }

    if (quote != NULL) {
        unsigned int offset2 = (unsigned int)strcspn(start, quote);
        if (offset > offset2) {
            offset = offset2;
        }
    }

    p = start + offset;

    /*
     * If not at end of string, we need to insert a null to terminate the
     * returned token.	See notes above.
     */
    if (*p != '\0') {
        if (strchr(whitespace, *p) == NULL) {
            rc = memmove_s(p + 1, strlen(p) + 1, p, strlen(p) + 1);
            securec_check_c(rc, "\0", "\0");
        }
        *p = '\0';
        string = p + 1;
    } else {
        /* at end of string, so no extra work */
        string = p;
    }

    return start;
}

/*
 * strip_quotes
 *
 * Remove quotes from the string at *source.  Leading and trailing occurrences
 * of 'quote' are removed; embedded double occurrences of 'quote' are reduced
 * to single occurrences; if 'escape' is not 0 then 'escape' removes special
 * significance of next character.
 *
 * Note that the source string is overwritten in-place.
 */
static void strip_quotes(char* source, char quote, char escape, int encoding)
{
    char* src = NULL;
    char* dst = NULL;

    psql_assert(source);
    psql_assert(quote);

    src = dst = source;

    if (*src && *src == quote) {
        src++; /* skip leading quote */
    }
    while (*src) {
        char c = *src;
        int i;

        if (c == quote && src[1] == '\0') {
            break; /* skip trailing quote */
        } else if (c == quote && src[1] == quote) {
            src++; /* process doubled quote */
        } else if (c == escape && src[1] != '\0') {
            src++; /* process escaped character */
        }
        i = PQmblen(src, encoding);
        while (i--) {
            *dst++ = *src++;
        }
    }

    *dst = '\0';
}

/*
 * quote_if_needed
 *
 * Opposite of strip_quotes().	If "source" denotes itself literally without
 * quoting or escaping, returns NULL.  Otherwise, returns a malloc'd copy with
 * quoting and escaping applied:
 *
 * source -			string to parse
 * entails_quote -	any of these present?  need outer quotes
 * quote -			doubled within string, affixed to both ends
 * escape -			doubled within string
 * encoding -		the active character-set encoding
 *
 * Do not use this as a substitute for PQescapeStringConn().  Use it for
 * strings to be parsed by strtokx() or psql_scan_slash_option().
 */
char* quote_if_needed(const char* source, const char* entails_quote, char quote, char escape, int encoding)
{
    const char* src = NULL;
    char* ret = NULL;
    char* dst = NULL;
    bool need_quotes = false;

    psql_assert(source);
    psql_assert(quote);

    src = source;
    dst = ret = (char*)pg_malloc(2 * strlen(src) + 3); /* excess */

    *dst++ = quote;

    while (*src) {
        char c = *src;
        int i;

        if (c == quote) {
            need_quotes = true;
            *dst++ = quote;
        } else if (c == escape) {
            need_quotes = true;
            *dst++ = escape;
        } else if (strchr(entails_quote, c) != NULL)
            need_quotes = true;

        i = PQmblen(src, encoding);
        while (i--) {
            *dst++ = *src++;
        }
    }

    *dst++ = quote;
    *dst = '\0';

    if (!need_quotes) {
        free(ret);
        ret = NULL;
    }

    return ret;
}

