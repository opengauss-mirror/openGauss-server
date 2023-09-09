/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/input.c
 */
#include "settings.h"
#include "postgres_fe.h"

#include <fcntl.h>
#include <limits.h>

#include "input.h"
#include "tab-complete.h"
#include "common.h"

/* Runtime options for turning off readline and history */
/* (of course there is no runtime command for doing that :) */
#ifdef USE_READLINE
bool useReadline = false;

#ifdef ENABLE_UT
#define static
#endif

/*
 * Preserve newlines in saved queries by mapping '\n' to NL_IN_HISTORY
 *
 * It is assumed NL_IN_HISTORY will never be entered by the user
 * nor appear inside a multi-byte string.    0x00 is not properly
 * handled by the readline routines so it can not be used
 * for this purpose.
 */
#define NL_IN_HISTORY 0x01
#endif

/*
 * gets_interactive()
 *
 * Gets a line of interactive input, using readline if desired.
 * The result is a malloc'd string.
 *
 * Caller *must* have set up sigint_interrupt_jmp before calling.
 */
char* gets_interactive(const char* prompt, PQExpBuffer query_buf)
{
#ifdef USE_READLINE
    if (useReadline) {
        char* result = NULL;

        /* Make current query_buf available to tab completion callback */
        tab_completion_query_buf = query_buf;

        /* Enable SIGINT to longjmp to sigint_interrupt_jmp */
        sigint_interrupt_enabled = true;

        /* On some platforms, readline is declared as readline(char *) */
        result = readline((char*)prompt);

        /* Disable SIGINT again */
        sigint_interrupt_enabled = false;

        /* Pure neatnik-ism */
        tab_completion_query_buf = NULL;

        return result;
    }
#endif

    fputs(prompt, stdout);
    (void)fflush(stdout);
    return gets_fromFile(stdin);
}

/*
 * Append the line to the history buffer, making sure there is a trailing '\n'
 */
void pg_append_history(const char* s, PQExpBuffer history_buf)
{
#ifdef USE_READLINE
    if (useReadline && s != NULL) {
        appendPQExpBufferStr(history_buf, s);
        if (!s[0] || s[strlen(s) - 1] != '\n')
            appendPQExpBufferChar(history_buf, '\n');
    }
#endif
}

/*
 * Emit accumulated history entry to readline's history mechanism,
 * then reset the buffer to empty.
 *
 * Note: we write nothing if history_buf is empty, so extra calls to this
 * function don't hurt.  There must have been at least one line added by
 * pg_append_history before we'll do anything.
 */
void pg_send_history(PQExpBuffer history_buf)
{
#ifdef USE_READLINE
    static char* prev_hist = NULL;

    char* s = history_buf->data;
    int i;

    /* Trim any trailing \n's (OK to scribble on history_buf) */
    for (i = strlen(s) - 1; i >= 0 && s[i] == '\n'; i--)
        ;
    s[(uint32)(i + 1)] = '\0';

    if (useReadline && s[0]) {
        if (((((unsigned int)pset.histcontrol) & hctl_ignorespace) && s[0] == ' ') ||
            ((((unsigned int)pset.histcontrol) & hctl_ignoredups) && (prev_hist != NULL) &&
                strcmp(s, prev_hist) == 0)) {
            /* Ignore this line as far as history is concerned */
        } else {
            /* Save each previous line for ignoredups processing */
            if (prev_hist != NULL)
                free(prev_hist);
            prev_hist = pg_strdup(s);
            /* And send it to readline */
            if (!SensitiveStrCheck(s) && canAddHist) {
                (void)add_history(s);
                /* Count lines added to history for use later */
            } else if (!canAddHist) {
                canAddHist = true;
            }
        }
    }

    resetPQExpBuffer(history_buf);
#endif
}

/*
 * gets_fromFile
 *
 * Gets a line of noninteractive input from a file (which could be stdin).
 * The result is a malloc'd string, or NULL on EOF or input error.
 *
 * Caller *must* have set up sigint_interrupt_jmp before calling.
 *
 * Note: we re-use a static PQExpBuffer for each call. This is to avoid
 * leaking memory if interrupted by SIGINT.
 */
char* gets_fromFile(FILE* source)
{
    static PQExpBuffer buffer = NULL;
    errno_t rc = 0;

    if (buffer == NULL) /* first time through? */
        buffer = createPQExpBuffer();
    else
        (void)resetPQExpBuffer(buffer);

    for (;;) {
        char* result = NULL;

        /* Enable SIGINT to longjmp to sigint_interrupt_jmp */
        sigint_interrupt_enabled = true;

        /* Database Security: Data importing/dumping support AES128. */
        /* Get some data */
        if (pset.decryptInfo.encryptInclude == false) {
            result = fgets(pset.decryptInfo.currLine, sizeof(pset.decryptInfo.currLine), source);
        } else {
            result = getLineFromAesEncryptFile(source, &pset.decryptInfo);
        }

        /* Disable SIGINT again */
        sigint_interrupt_enabled = false;

        /* EOF or error? */
        if (result == NULL) {
            if (ferror(source)) {
                psql_error("could not read from input file: %s\n", strerror(errno));
                return NULL;
            }
            break;
        }

        /*
         * Data importing/dumping support AES128 through VPP SSL.
         * when decrypt text is longer than MAX_DECRYPT_BUFF_LEN
         * We use decryptBuff here.
         */
        if (pset.decryptInfo.encryptInclude == true) {
            appendPQExpBufferStr(buffer, (char*)pset.decryptInfo.decryptBuff);
            free(pset.decryptInfo.decryptBuff);
            pset.decryptInfo.decryptBuff = NULL;
        } else {
            appendPQExpBufferStr(buffer, pset.decryptInfo.currLine);
        }

        /* Clear password related memory to avoid leaks when core. */
        if (pset.cur_cmd_interactive) {
            rc = memset_s(
                pset.decryptInfo.currLine, sizeof(pset.decryptInfo.currLine), 0, sizeof(pset.decryptInfo.currLine));
            securec_check_c(rc, "\0", "\0");
        }

        if (PQExpBufferBroken(buffer)) {
            psql_error("out of memory\n");
            return NULL;
        }

        /* EOL? */
        if (buffer->len > 0 && buffer->data[buffer->len - 1] == '\n') {
            char* line = NULL;
            buffer->data[buffer->len - 1] = '\0';
            line = pg_strdup(buffer->data);

            /* Clear password related memory to avoid leaks when core. */
            if (pset.cur_cmd_interactive) {
                rc = memset_s(buffer->data, strlen(buffer->data), 0, strlen(buffer->data));
                securec_check_c(rc, "\0", "\0");
            }
            return line;
        }
    }

    if (buffer->len > 0) { /* EOF after reading some bufferload(s) */
        char* line = NULL;
        line = pg_strdup(buffer->data);

        /* Clear password related memory to avoid leaks when core. */
        if (pset.cur_cmd_interactive) {
            rc = memset_s(buffer->data, strlen(buffer->data), 0, strlen(buffer->data));
            securec_check_c(rc, "\0", "\0");
        }
        return line;
    }

    /* EOF, so return null */
    return NULL;
}

bool SensitiveStrCheck(const char* target)
{
    int j = 0;
    int target_len = (int)strlen(target);
    char* target_copy = (char*)pg_malloc(target_len + 1);
    errno_t rc;
    rc = strncpy_s(target_copy, target_len + 1, target, target_len);
    securec_check_c(rc, "\0", "\0");
    target_copy[target_len] = '\0';
    for (j = 0; j < target_len; j++) {
        target_copy[j] = toupper(target_copy[j]);
    }
    
    if (strstr(target_copy, "PASSWORD") != NULL || strstr(target_copy, "IDENTIFIED") != NULL ||
        strstr(target_copy, "GS_ENCRYPT_AES128") != NULL || strstr(target_copy, "GS_DECRYPT_AES128") != NULL ||
        strstr(target_copy, "GS_ENCRYPT") != NULL || strstr(target_copy, "GS_DECRYPT") != NULL ||
        strstr(target_copy, "AES_ENCRYPT") != NULL || strstr(target_copy, "AES_DECRYPT") != NULL ||
        strstr(target_copy, "PG_CREATE_PHYSICAL_REPLICATION_SLOT_EXTERN") != NULL ||
        strstr(target_copy, "SECRET_ACCESS_KEY") != NULL ||
        strstr(target_copy, "SECRETKEY") != NULL || strstr(target_copy, "CREATE_CREDENTIAL") != NULL ||
        strstr(target_copy, "ACCESS_KEY") != NULL || strstr(target_copy, "SECRET_ACCESS_KEY") != NULL) {
        free(target_copy);
        return TRUE;
    } else {
        free(target_copy);
        return FALSE;
    }
}

void setHistSize(const char* targetName, const char* targetValue, bool setToDefault)
{
#ifndef ENABLE_LLT
    char* end = NULL;
    long int result;
#define MAXHISTSIZE 500
#define DEFHISTSIZE 32
    if (targetName == NULL) {
        return;
    }
    if (strcmp(targetName, "HISTSIZE") == 0) {
        if (!setToDefault) {
            if (targetValue == NULL || strlen(targetValue) == 0) {
                fprintf(stderr, "warning:\"HISTSIZE\" is not changed,because its value can not be null\n");
                return;
            } else {
                errno = 0;
                result = strtol(targetValue, &end, 0);
                if ((errno == ERANGE && (result == LONG_MAX || result == LONG_MIN)) || (errno != 0 && result == 0)) {
                    fprintf(stderr, "warning:\"HISTSIZE\" is not changed,because its value overflows\n");
                    return;
                }
                if (*end || result < 0) {
                    fprintf(stderr, "warning:\"HISTSIZE\" is not changed,because its value must be positive integer\n");
                    return;
                }
            }
            if (result > MAXHISTSIZE) {
                fprintf(stderr, "warning:\"HISTSIZE\" is set to 500,because its value can not be greater than 500\n");
                result = MAXHISTSIZE;
            }
        } else {
            result = DEFHISTSIZE;
        }
        stifle_history((int)result);
    }
#endif
}

/*
 * Put any startup stuff related to input in here. It's good to maintain
 * abstraction this way.
 *
 * The only "flag" right now is 1 for use readline & history.
 */
void initializeInput(int flags)
{
#ifdef USE_READLINE
    flags &= useReadline;

    if (flags & 1) {
        /* these two things must be done in this order: */
        initialize_readline();
        rl_variable_bind ("enable-meta-key", "off");
        rl_initialize();
    }
#endif
}
