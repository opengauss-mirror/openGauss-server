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
#include <assert.h>

#include "input.h"
#include "tab-complete.h"
#include "common.h"

#ifndef WIN32
#define PSQLHISTORY ".gsql_history"
#else
#define PSQLHISTORY "gsql_history"
#endif

/* Runtime options for turning off readline and history */
/* (of course there is no runtime command for doing that :) */
#ifdef USE_READLINE
bool useReadline = false;
static int history_lines_added;
static char* psql_history;
static bool useHistory;
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

bool canAddHistory(const char* target);
void setHistSize(const char* targetName, char* targetValue, bool setToDefault);
void finishInput(void);

/*
 * gets_interactive()
 *
 * Gets a line of interactive input, using readline if desired.
 * The result is a malloc'd string.
 *
 * Caller *must* have set up sigint_interrupt_jmp before calling.
 */
char* gets_interactive(const char* prompt)
{
#ifdef USE_READLINE
    if (useReadline) {
        char* result = NULL;

        /* Enable SIGINT to longjmp to sigint_interrupt_jmp */
        sigint_interrupt_enabled = true;

        /* On some platforms, readline is declared as readline(char *) */
        result = readline((char*)prompt);

        /* Disable SIGINT again */
        sigint_interrupt_enabled = false;

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
            if (canAddHistory(s) && canAddHist) {
                (void)add_history(s);
                /* Count lines added to history for use later */
                history_lines_added++;
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
        resetPQExpBuffer(buffer);

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

bool canAddHistory(const char* target)
{
#ifdef USE_READLINE
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
    if (NULL != strstr(target_copy, "PASSWORD") || NULL != strstr(target_copy, "IDENTIFIED")) {
        free(target_copy);
        return FALSE;
    } else if (NULL != strstr(target_copy, "GS_ENCRYPT_AES128") || NULL != strstr(target_copy, "GS_DECRYPT_AES128")) {
        free(target_copy);
        return FALSE;
    } else {
        free(target_copy);
        return TRUE;
    }
#endif
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

#ifdef USE_READLINE

/*
 * Macros to iterate over each element of the history list in order
 *
 * You would think this would be simple enough, but in its inimitable fashion
 * libedit has managed to break it: in libreadline we must use next_history()
 * to go from oldest to newest, but in libedit we must use previous_history().
 * To detect what to do, we make a trial call of previous_history(): if it
 * fails, then either next_history() is what to use, or there's zero or one
 * history entry so that it doesn't matter which direction we go.
 *
 * In case that wasn't disgusting enough: the code below is not as obvious as
 * it might appear.  In some libedit releases history_set_pos(0) fails until
 * at least one add_history() call has been done.  This is not an issue for
 * printHistory() or encode_history(), which cannot be invoked before that has
 * happened.  In decode_history(), that's not so, and what actually happens is
 * that we are sitting on the newest entry to start with, previous_history()
 * fails, and we iterate over all the entries using next_history().  So the
 * decode_history() loop iterates over the entries in the wrong order when
 * using such a libedit release, and if there were another attempt to use
 * BEGIN_ITERATE_HISTORY() before some add_history() call had happened, it
 * wouldn't work.  Fortunately we don't care about either of those things.
 *
 * Usage pattern is:
 *
 *        BEGIN_ITERATE_HISTORY(varname);
 *        {
 *            loop body referencing varname->line;
 *        }
 *        END_ITERATE_HISTORY();
 */
#define BEGIN_ITERATE_HISTORY(VARNAME) \
    do { \
        HIST_ENTRY* VARNAME; \
        bool use_prev_; \
        \
        history_set_pos(0); \
        use_prev_ = (previous_history() != NULL); \
        history_set_pos(0); \
        for (VARNAME = current_history(); VARNAME != NULL; \
             VARNAME = use_prev_ ? previous_history() : next_history()) \
        { \
            (void) 0

#define END_ITERATE_HISTORY() \
        } \
    } while(0)


/*
 * Convert newlines to NL_IN_HISTORY for safe saving in readline history file
 */
static void encode_history(void)
{
    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        char* cur_ptr;

        /* some platforms declare HIST_ENTRY.line as const char * */
        for (cur_ptr = (char *) cur_hist->line; *cur_ptr; cur_ptr++) {
            if (*cur_ptr == '\n')
                *cur_ptr = NL_IN_HISTORY;
        }
    }
    END_ITERATE_HISTORY();
}

/*
 * Reverse the above encoding
 */
static void decode_history(void)
{
    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        char* cur_ptr;

        /* some platforms declare HIST_ENTRY.line as const char * */
        for (cur_ptr = (char *)cur_hist->line; *cur_ptr; cur_ptr++) {
            if (*cur_ptr == NL_IN_HISTORY)
                *cur_ptr = '\n';
        }
    }
    END_ITERATE_HISTORY();
}
#endif   /* USE_READLINE */

/*
 * Put any startup stuff related to input in here. It's good to maintain
 * abstraction this way.
 *
 * The only "flag" right now is 1 for use readline & history.
 */
void initializeInput(int flags)
{
#ifdef USE_READLINE

#ifndef ENABLE_MULTIPLE_NODES
    flags &= useReadline;
#endif

    if (flags & 1) {
        const char* histfile = NULL;
        char home[MAXPGPATH];

        useReadline = true;

        /* these two things must be done in this order: */
        initialize_readline();
        rl_variable_bind ("enable-meta-key", "off");
        rl_initialize();
                
        useHistory = true;
        using_history();
        history_lines_added = 0;

        histfile = GetVariable(pset.vars, "HISTFILE");

        if (histfile == NULL) {
            char* envhist;

            envhist = getenv("PSQL_HISTORY");
            if (envhist != NULL && strlen(envhist) > 0)
                histfile = envhist;
        }

        if (histfile == NULL) {
            if (get_home_path(home,MAXPGPATH)) {
                /*
                 * add 2 more bytes, 1 for '\0', 1 for '/'.
                 * the max length of home is MAXPGPATH, so the actualLen won't overflow.
                 */
                int actualLen = strlen(home) + strlen(PSQLHISTORY) + 2;
                psql_history = (char*)pg_malloc(actualLen);
                int rc = snprintf_s(psql_history, actualLen, actualLen - 1, "%s/%s", home, PSQLHISTORY);
                securec_check_ss_c(rc, "", "");
            }
        } else {
            check_env_value(histfile);
            psql_history = pg_strdup(histfile);
            expand_tilde(&psql_history);
        }

        if (psql_history) {
            read_history(psql_history);
            decode_history();
        }
    }
#endif
}


/*
 * This function saves the readline history when psql exits.
 *
 * fname: pathname of history file.  (Should really be "const char *",
 * but some ancient versions of readline omit the const-decoration.)
 *
 * max_lines: if >= 0, limit history file to that many entries.
 */
#ifdef USE_READLINE
static bool saveHistory(char* fname, int max_lines)
{
    int errnum;

    /*
     * Suppressing the write attempt when HISTFILE is set to /dev/null may
     * look like a negligible optimization, but it's necessary on e.g. Darwin,
     * where write_history will fail because it tries to chmod the target
     * file.
     */
    if (strcmp(fname, DEVNULL) != 0) {
        /*
         * Encode \n, since otherwise readline will reload multiline history
         * entries as separate lines.  (libedit doesn't really need this, but
         * we do it anyway since it's too hard to tell which implementation we
         * are using.)
         */
        encode_history();

        /*
         * On newer versions of libreadline, truncate the history file as
         * needed and then append what we've added.  This avoids overwriting
         * history from other concurrent sessions (although there are still
         * race conditions when two sessions exit at about the same time). If
         * we don't have those functions, fall back to write_history().
         */
#if defined(HAVE_HISTORY_TRUNCATE_FILE) && defined(HAVE_APPEND_HISTORY)
        {
            int nlines;
            int fd;

            /* truncate previous entries if needed */
            if (max_lines >= 0) {
                nlines = Max(max_lines - history_lines_added, 0);
                (void) history_truncate_file(fname, nlines);
            }
            /* append_history fails if file doesn't already exist :-( */
            fd = open(fname, O_CREAT | O_WRONLY | PG_BINARY, 0600);
            if (fd >= 0)
                close(fd);
            /* append the appropriate number of lines */
            if (max_lines >= 0)
                nlines = Min(max_lines, history_lines_added);
            else
                nlines = history_lines_added;
            errnum = append_history(nlines, fname);
            if (errnum == 0)
                return true;
        }
#else                            /* don't have append support */
        {
            /* truncate what we have ... */
            if (max_lines >= 0)
                stifle_history(max_lines);
            /* ... and overwrite file.  Tough luck for concurrent sessions. */
            errnum = write_history(fname);
            if (errnum == 0)
                return true;
        }
#endif

        psql_error("could not save history to file \"%s\": %s\n",
                   fname, strerror(errnum));
    }
    return false;
}
#endif



/*
 * Print history to the specified file, or to the console if fname is NULL
 * (psql \s command)
 *
 * We used to use saveHistory() for this purpose, but that doesn't permit
 * use of a pager; moreover libedit's implementation behaves incompatibly
 * (preferring to encode its output) and may fail outright when the target
 * file is specified as /dev/tty.
 */
bool printHistory(const char *fname, unsigned short int pager)
{
#ifdef USE_READLINE
    FILE* output;
    bool is_pager;

    if (!useHistory)
        return false;

    if (fname == NULL) {
        /* use pager, if enabled, when printing to console */
        output = PageOutput(INT_MAX, pager);
        is_pager = true;
    } else {
        output = fopen(fname, "w");
        if (output == NULL) {
            psql_error("could not save history to file \"%s\": %s\n",
                       fname, strerror(errno));
            return false;
        }
        is_pager = false;
    }

    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        fprintf(output, "%s\n", cur_hist->line);
    }
    END_ITERATE_HISTORY();

    if (is_pager)
        ClosePager(output);
    else
        fclose(output);

    return true;
#else
    psql_error("history is not supported by this installation\n");
    return false;
#endif
}


void finishInput(void)
{
#ifdef USE_READLINE
    if (useHistory && psql_history) {
        int hist_size;

        hist_size = GetVariableNum(pset.vars, "HISTSIZE", 500, -1, true);
        (void)saveHistory(psql_history, hist_size);
        free(psql_history);
        psql_history = NULL;
    }
#endif
}

