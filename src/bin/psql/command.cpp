/*
 * psql - the openGauss interactive terminal
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 * src/bin/psql/command.c
 */
#include "settings.h"
#include "postgres_fe.h"
#include "command.h"

#ifdef __BORLANDC__ /* needed for BCC */
#undef mkdir
#endif

#include <ctype.h>
#include <math.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#ifndef WIN32
#include <sys/types.h> /* for umask() */
#include <sys/stat.h>  /* for stat() */
#include <sys/time.h>
#include <fcntl.h>  /* open() flags */
#include <unistd.h> /* for geteuid(), getpid(), stat() */
#else
#include <win32.h>
#include <io.h>
#include <fcntl.h>
#include <direct.h>
#include <sys/types.h> /* for umask() */
#include <sys/stat.h>  /* for stat() */
#include <sys/time.h>
#endif
#ifdef USE_SSL
#ifndef WIN32
#include "openssl/ssl.h"
#else
#include "ssl/win32/ssl.h"
#endif
#endif

#include "portability/instr_time.h"

#include "libpq/libpq-fe.h"
#include "libpq/pqexpbuffer.h"
#include "dumputils.h"

#include "common.h"
#include "copy.h"
#include "describe.h"
#include "help.h"
#include "input.h"
#include "large_obj.h"
#include "mainloop.h"
#include "print.h"
#include "psqlscan.h"
#include "variables.h"
#include "securec.h"

/* Database Security: Data importing/dumping support AES128. */
#include <time.h>
#include "pgtime.h"

#ifdef ENABLE_UT
#define static
#endif

/* functions for use in this file */
static backslashResult exec_command(const char* cmd, PsqlScanState scan_state, PQExpBuffer query_buf);
static bool do_edit(const char* filename_arg, PQExpBuffer query_buf, int lineno, bool* edited);
static bool do_connect(char* dbname, char* user, char* host, char* port);
static bool do_shell(const char* command);
static bool lookup_function_oid(PGconn* conn, const char* desc, Oid* foid);
static bool get_create_function_cmd(PGconn* conn, Oid oid, PQExpBuffer buf);
static int strip_lineno_from_funcdesc(char* func);
static void minimal_error_message(PGresult* res);

static void printSSLInfo(void);

#ifndef ENABLE_LITE_MODE
/* Show notice about when the password expired time will come. */
static void show_password_notify(PGconn* conn);
#endif

#ifdef WIN32
static void checkWin32Codepage(void);
#endif

/* functions for use in clear senstive memory. */
static void clear_sensitive_memory(char* strings, bool freeflag);

extern void check_env_value(const char* input_env_value);

/* ----------
 * HandleSlashCmds:
 *
 * Handles all the different commands that start with '\'.
 * Ordinarily called by MainLoop().
 *
 * scan_state is a lexer working state that is set to continue scanning
 * just after the '\'.  The lexer is advanced past the command and all
 * arguments on return.
 *
 * 'query_buf' contains the query-so-far, which may be modified by
 * execution of the backslash command (for example, \r clears it).
 * query_buf can be NULL if there is no query so far.
 *
 * Returns a status code indicating what action is desired, see command.h.
 * ----------
 */

backslashResult HandleSlashCmds(PsqlScanState scan_state, PQExpBuffer query_buf)
{
    backslashResult status = PSQL_CMD_SKIP_LINE;
    char* cmd = NULL;
    char* arg = NULL;

    psql_assert(scan_state);

    /* Parse off the command name */
    cmd = psql_scan_slash_command(scan_state);

    /* And try to execute it */
    status = exec_command(cmd, scan_state, query_buf);

    if (status == PSQL_CMD_UNKNOWN) {
        if (pset.cur_cmd_interactive) {
            fprintf(stderr, _("Invalid command \\%s. Try \\? for help.\n"), cmd);
        } else {
            psql_error("invalid command \\%s\n", cmd);
        }
        status = PSQL_CMD_ERROR;
    }

    if (status != PSQL_CMD_ERROR) {
        /* eat any remaining arguments after a valid command */
        /* note we suppress evaluation of backticks here */
        while ((arg = psql_scan_slash_option(scan_state, OT_NO_EVAL, NULL, false)) != NULL) {
            psql_error("\\%s: extra argument \"%s\" ignored\n", cmd, arg);
            free(arg);
            arg = NULL;
        }
    } else {
        /* silently throw away rest of line after an erroneous command */
        while ((arg = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false)) != NULL) {
            free(arg);
            arg = NULL;
        }
    }

    /* if there is a trailing \\, swallow it */
    psql_scan_slash_command_end(scan_state);

    free(cmd);
    cmd = NULL;

    /* some commands write to queryFout, so make sure output is sent */
    (void)fflush(pset.queryFout);

    return status;
}

/*
 * Read and interpret an argument to the \connect slash command.
 */
static char* read_connect_arg(PsqlScanState scan_state)
{
    char* result = NULL;
    char quote;

    /*
     * Ideally we should treat the arguments as SQL identifiers.  But for
     * backwards compatibility with 7.2 and older pg_dump files, we have to
     * take unquoted arguments verbatim (don't downcase them). For now,
     * double-quoted arguments may be stripped of double quotes (as if SQL
     * identifiers).  By 7.4 or so, pg_dump files can be expected to
     * double-quote all mixed-case \connect arguments, and then we can get rid
     * of OT_SQLIDHACK.
     */
    result = psql_scan_slash_option(scan_state, OT_SQLID, &quote, true);
    if (NULL == result) {
        return NULL;
    }
    if (quote != 0) {
        return result;
    }

    if (*result == '\0' || strcmp(result, "-") == 0) {
        free(result);
        result = NULL;
        return NULL;
    }
    return result;
}

/*
 * Subroutine to actually try to execute a backslash command.
 */
static backslashResult exec_command(const char* cmd, PsqlScanState scan_state, PQExpBuffer query_buf)
{
    bool success = true; /* indicate here if the command ran ok or
                          * failed */
    backslashResult status = PSQL_CMD_SKIP_LINE;
    /* Database Security: Data importing/dumping support AES128. */
    int tmplen = 0;
    int tmpkeylen = 0;
    char* dencrypt_key = NULL;
    bool isIllegal = false;
    errno_t rc = 0;

    if (NULL == cmd) {
        psql_error("could not get cmd in function exec_command, so exit\n");
        exit(EXIT_FAILURE);
    }

    /* Clear Sensitive info in query_buf for security */
    if (query_buf != NULL && query_buf->len > 0 && SensitiveStrCheck(query_buf->data)) {
        resetPQExpBuffer(query_buf);
    }
    
    /*
     * \a -- toggle field alignment This makes little sense but we keep it
     * around.
     */
    if (strcmp(cmd, "a") == 0) {
        if (pset.popt.topt.format != PRINT_ALIGNED) {
            success = do_pset("format", "aligned", &pset.popt, pset.quiet);
        } else {
            success = do_pset("format", "unaligned", &pset.popt, pset.quiet);
        }
    }

    /* \C -- override table title (formerly change HTML caption) */
    else if (strcmp(cmd, "C") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        success = do_pset("title", opt, &pset.popt, pset.quiet);
        if (NULL != opt) {
            free(opt);
            opt = NULL;
        }
    }

    /*
     * \c or \connect -- connect to database using the specified parameters.
     *
     * \c dbname user host port
     *
     * If any of these parameters are omitted or specified as '-', the current
     * value of the parameter will be used instead. If the parameter has no
     * current value, the default value for that parameter will be used. Some
     * examples:
     *
     * \c - - hst		Connect to current database on current port of host
     * "hst" as current user. \c - usr - prt   Connect to current database on
     * "prt" port of current host as user "usr". \c dbs			  Connect to
     * "dbs" database on current port of current host as current user.
     */
    else if (strcmp(cmd, "c") == 0 || strcmp(cmd, "connect") == 0) {
        char *opt1 = NULL, *opt2 = NULL, *opt3 = NULL, *opt4 = NULL;

        opt1 = read_connect_arg(scan_state);
        opt2 = read_connect_arg(scan_state);
        opt3 = read_connect_arg(scan_state);
        opt4 = read_connect_arg(scan_state);

        success = do_connect(opt1, opt2, opt3, opt4);

        if (NULL != opt1) {
            free(opt1);
            opt1 = NULL;
        }

        if (NULL != opt2) {
            free(opt2);
            opt2 = NULL;
        }

        if (NULL != opt3) {
            free(opt3);
            opt3 = NULL;
        }

        if (NULL != opt4) {
            free(opt4);
            opt4 = NULL;
        }
    }

    /* \cd */
    else if (strcmp(cmd, "cd") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
        char* dir = NULL;

        if (NULL != opt) {
            dir = opt;
        } else {
#ifndef WIN32
            struct passwd* pw = NULL;

            pw = getpwuid(geteuid());
            if (NULL == pw) {
                psql_error("could not get home directory: %s\n", strerror(errno));
                exit(EXIT_FAILURE);
            }
            dir = pw->pw_dir;
#else  /* WIN32 */

            /*
             * On Windows, 'cd' without arguments prints the current
             * directory, so if someone wants to code this here instead...
             */
            dir = "/";
#endif /* WIN32 */
        }
        canonicalize_path(dir);
        if (chdir(dir) == -1) {
            psql_error("\\%s: could not change directory to \"%s\": %s\n", cmd, dir, strerror(errno));
            success = false;
        }

        if (NULL != pset.dirname) {
            free(pset.dirname);
        }
        pset.dirname = pg_strdup(dir);
        canonicalize_path(pset.dirname);

        if (NULL != opt) {
            free(opt);
        }
        opt = NULL;
    }

    /* \conninfo -- display information about the current connection */
    else if (strcmp(cmd, "conninfo") == 0) {
        char* db = PQdb(pset.db);
        char* host = PQhost(pset.db);

        if (db == NULL) {
            printf(_("You are currently not connected to a database.\n"));
        } else {
            if (host == NULL) {
                host = DEFAULT_PGSOCKET_DIR;
            }
            /* If the host is an absolute path, the connection is via socket */
            if (is_absolute_path(host)) {
                printf(_("You are connected to database \"%s\" as user \"%s\" via socket in \"%s\" at port \"%s\".\n"),
                    db,
                    PQuser(pset.db),
                    host,
                    PQport(pset.db));
            } else {
                printf(_("You are connected to database \"%s\" as user \"%s\" on host \"%s\" at port \"%s\".\n"),
                    db,
                    PQuser(pset.db),
                    host,
                    PQport(pset.db));
            }
        }
    }

    /* \copy */
    else if (pg_strcasecmp(cmd, "copy") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false);
        success = do_copy(opt);
        if (NULL != opt) {
            free(opt);
            opt = NULL;
        }
    }

    /* \copyright */
    else if (strcmp(cmd, "copyright") == 0) {
        print_copyright();
    }
    /* \d* commands */
    else if (cmd[0] == 'd') {
        char* pattern = NULL;
        bool show_verbose = false;
        bool show_system = false;

        /* We don't do SQLID reduction on the pattern yet */
        pattern = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        show_verbose = strchr(cmd, '+') != NULL ? true : false;
        show_system = strchr(cmd, 'S') != NULL ? true : false;

        switch (cmd[1]) {
            case '\0':
            case '+':
            case 'S':
                if (NULL != pattern)
                    success = describeTableDetails(pattern, show_verbose, show_system);
                else
                    /* standard listing of interesting things */
                    success = listTables("tvsEmeo", NULL, show_verbose, show_system);
                break;
            case 'a':
                success = describeAggregates(pattern, show_verbose, show_system);
                break;
            case 'b':
                success = describeTablespaces(pattern, show_verbose);
                break;
            case 'c':
                success = listConversions(pattern, show_verbose, show_system);
                break;
            case 'C':
                success = listCasts(pattern, show_verbose);
                break;
            case 'd':
                if (strncmp(cmd, "ddp", 3) == 0)
                    success = listDefaultACLs(pattern);
                else
                    success = objectDescription(pattern, show_system);
                break;
            case 'D':
                success = listDomains(pattern, show_verbose, show_system);
                break;
            case 'f': /* function subsystem */
                switch (cmd[2]) {
                    case '\0':
                    case '+':
                    case 'S':
                    case 'a':
                    case 'n':
                    case 't':
                    case 'w':
                        success = describeFunctions(&cmd[2], pattern, show_verbose, show_system);
                        break;
                    default:
                        status = PSQL_CMD_UNKNOWN;
                        break;
                }
                break;
            case 'g':
                /* no longer distinct from \du */
                success = describeRoles(pattern, show_verbose);
                break;
            case 'l':
                success = do_lo_list();
                break;
            case 'L':
                success = listLanguages(pattern, show_verbose, show_system);
                break;
            case 'n':
                success = listSchemas(pattern, show_verbose, show_system);
                break;
            case 'o':
                success = describeOperators(pattern, show_system);
                break;
            case 'O':
                success = listCollations(pattern, show_verbose, show_system);
                break;
            case 'p':
                success = permissionsList(pattern);
                break;
            case 'T':
                success = describeTypes(pattern, show_verbose, show_system);
                break;
            case 't':
            case 'v':
            case 'm':
            case 'i':
            case 's':
            case 'E':
                success = listTables(&cmd[1], pattern, show_verbose, show_system);
                break;
            case 'r':
                if (cmd[2] == 'd' && cmd[3] == 's') {
                    char* pattern2 = NULL;

                    if (NULL != pattern) {
                        pattern2 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
                    }
                    success = listDbRoleSettings(pattern, pattern2);
                } else {
                    status = PSQL_CMD_UNKNOWN;
                }
                break;
            case 'u':
                success = describeRoles(pattern, show_verbose);
                break;
            case 'F': /* text search subsystem */
                switch (cmd[2]) {
                    case '\0':
                    case '+':
                        success = listTSConfigs(pattern, show_verbose);
                        break;
                    case 'p':
                        success = listTSParsers(pattern, show_verbose);
                        break;
                    case 'd':
                        success = listTSDictionaries(pattern, show_verbose);
                        break;
                    case 't':
                        success = listTSTemplates(pattern, show_verbose);
                        break;
                    default:
                        status = PSQL_CMD_UNKNOWN;
                        break;
                }
                break;
            case 'e': /* SQL/MED subsystem */
                switch (cmd[2]) {
                    case 'd':
                        success = listDataSource(pattern, show_verbose);
                        break;
                    case 's':
                        success = listForeignServers(pattern, show_verbose);
                        break;
                    case 'u':
                        success = listUserMappings(pattern, show_verbose);
                        break;
                    case 'w':
                        success = listForeignDataWrappers(pattern, show_verbose);
                        break;
                    case 't':
                        success = listForeignTables(pattern, show_verbose);
                        break;
                    default:
                        status = PSQL_CMD_UNKNOWN;
                        break;
                }
                break;
            case 'x': /* Extensions */
                if (show_verbose) {
                    success = listExtensionContents(pattern);
                } else {
                    success = listExtensions(pattern);
                }
                break;
            default:
                status = PSQL_CMD_UNKNOWN;
        }

        if (NULL != pattern) {
            free(pattern);
        }
        pattern = NULL;
    }

    /*
     * \e or \edit -- edit the current query buffer, or edit a file and make
     * it the query buffer
     */
    else if (strcmp(cmd, "e") == 0 || strcmp(cmd, "edit") == 0) {
        if (query_buf == NULL) {
            psql_error("no query buffer\n");
            status = PSQL_CMD_ERROR;
        } else {
            char* fname = NULL;
            char* ln = NULL;
            int lineno = -1;

            fname = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
            if (NULL != fname) {
                /* try to get separate lineno arg */
                ln = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
                if (ln == NULL) {
                    /* only one arg; maybe it is lineno not fname */
                    if (fname[0] && strspn(fname, "0123456789") == strlen(fname)) {
                        /* all digits, so assume it is lineno */
                        ln = fname;
                        fname = NULL;
                    }
                }
            }
            if (NULL != ln) {
                lineno = atoi(ln);
                if (lineno < 1) {
                    psql_error("invalid line number: %s\n", ln);
                    status = PSQL_CMD_ERROR;
                }
            }
            if (status != PSQL_CMD_ERROR) {
                expand_tilde(&fname);
                if (NULL != fname) {
                    canonicalize_path(fname);
                }
                if (do_edit(fname, query_buf, lineno, NULL)) {
                    status = PSQL_CMD_NEWEDIT;
                } else {
                    status = PSQL_CMD_ERROR;
                }
            }
            if (NULL != fname) {
                free(fname);
            }
            fname = NULL;
            if (NULL != ln) {
                free(ln);
            }
            ln = NULL;
        }
    }

    /*
     * \ef -- edit the named function, or present a blank CREATE FUNCTION
     * template if no argument is given
     */
    else if (strcmp(cmd, "ef") == 0) {
        int lineno = -1;

        if (pset.sversion < 80400) {
            psql_error("The server (version %d.%d) does not support editing function source.\n",
                pset.sversion / 10000,
                (pset.sversion / 100) % 100);
            status = PSQL_CMD_ERROR;
        } else if (query_buf == NULL) {
            psql_error("no query buffer\n");
            status = PSQL_CMD_ERROR;
        } else {
            char* func = NULL;
            Oid foid = InvalidOid;

            func = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, true);
            lineno = strip_lineno_from_funcdesc(func);
            if (lineno == 0) {
                /* error already reported */
                status = PSQL_CMD_ERROR;
            } else if (func == NULL) {
                /* set up an empty command to fill in */
                printfPQExpBuffer(query_buf,
                    "CREATE FUNCTION ( )\n"
                    " RETURNS \n"
                    " LANGUAGE \n"
                    " -- common options:  IMMUTABLE  STABLE  STRICT  SECURITY DEFINER\n"
                    "AS $function$\n"
                    "\n$function$\n");
            } else if (!lookup_function_oid(pset.db, func, &foid)) {
                /* error already reported */
                status = PSQL_CMD_ERROR;
            } else if (!get_create_function_cmd(pset.db, foid, query_buf)) {
                /* error already reported */
                status = PSQL_CMD_ERROR;
            } else if (lineno > 0) {
                /*
                 * lineno "1" should correspond to the first line of the
                 * function body.  We expect that pg_get_functiondef() will
                 * emit that on a line beginning with "AS ", and that there
                 * can be no such line before the real start of the function
                 * body.  Increment lineno by the number of lines before that
                 * line, so that it becomes relative to the first line of the
                 * function definition.
                 */
                const char* lines = query_buf->data;

                while (*lines != '\0') {
                    if (strncmp(lines, "AS ", 3) == 0) {
                        break;
                    }
                    lineno++;
                    /* find start of next line */
                    lines = strchr(lines, '\n');
                    if (NULL == lines) {
                        break;
                    }
                    lines++;
                }
            }

            if (NULL != func) {
                free(func);
            }
            func = NULL;
        }

        if (status != PSQL_CMD_ERROR) {
            bool edited = false;

            if (!do_edit(NULL, query_buf, lineno, &edited)) {
                status = PSQL_CMD_ERROR;
            } else if (!edited) {
                puts(_("No changes"));
            } else {
                status = PSQL_CMD_NEWEDIT;
            }
        }
    }

    /* \echo and \qecho */
    else if (strcmp(cmd, "echo") == 0 || strcmp(cmd, "qecho") == 0) {
        char* value = NULL;
        char quoted;
        bool no_newline = false;
        bool first = true;
        FILE* fout = NULL;

        if (strcmp(cmd, "qecho") == 0) {
            fout = pset.queryFout;
        } else {
            fout = stdout;
        }
        while ((value = psql_scan_slash_option(scan_state, OT_NORMAL, &quoted, false)) != NULL) {
            if (!quoted && strcmp(value, "-n") == 0) {
                no_newline = true;
            } else {
                if (first) {
                    first = false;
                } else {
                    fputc(' ', fout);
                }
                fputs(value, fout);
            }

            free(value);
            value = NULL;
        }
        if (!no_newline) {
            fputs("\n", fout);
        }
    }

    /* \encoding -- set/show client side encoding */
    else if (strcmp(cmd, "encoding") == 0) {
        char* encoding = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == encoding) {
            /* show encoding */
            puts(pg_encoding_to_char(pset.encoding));
        } else {
            /* set encoding */
            if (PQsetClientEncoding(pset.db, encoding) == -1) {
                psql_error("%s: invalid encoding name or conversion procedure not found\n", encoding);
            } else {
                /* save encoding info into psql internal data */
                pset.encoding = PQclientEncoding(pset.db);
                pset.popt.topt.encoding = pset.encoding;
                if (!SetVariable(pset.vars, "ENCODING", pg_encoding_to_char(pset.encoding))) {
                    psql_error("set variable %s failed.\n", "ENCODING");
                }
            }
            free(encoding);
            encoding = NULL;
        }
    }

    /* \f -- change field separator */
    else if (strcmp(cmd, "f") == 0) {
        char* fname = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        success = do_pset("fieldsep", fname, &pset.popt, pset.quiet);
        if (NULL != fname) {
            free(fname);
            fname = NULL;
        }
    }

    /* \g means send query */
    else if (strcmp(cmd, "g") == 0) {
        char* fname = psql_scan_slash_option(scan_state, OT_FILEPIPE, NULL, false);

        if (NULL == fname) {
            pset.gfname = NULL;
        } else {
            expand_tilde(&fname);
            pset.gfname = pg_strdup(fname);
        }
        if (NULL != fname) {
            free(fname);
            fname = NULL;
        }
        status = PSQL_CMD_SEND;
    }

    /* help */
    else if (strcmp(cmd, "h") == 0 || strcmp(cmd, "help") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false);
        size_t len;

        /* strip any trailing spaces and semicolons */
        if (NULL != opt) {
            len = strlen(opt);
            while (len > 0 && (isspace((unsigned char)opt[len - 1]) || opt[len - 1] == ';'))
                opt[--len] = '\0';
        }
        helpSQL(opt, pset.popt.topt.pager);
        if (NULL != opt) {
            free(opt);
            opt = NULL;
        }
    }

    /* HTML mode */
    else if (strcmp(cmd, "H") == 0 || strcmp(cmd, "html") == 0) {
        if (pset.popt.topt.format != PRINT_HTML)
            success = do_pset("format", "html", &pset.popt, pset.quiet);
        else
            success = do_pset("format", "aligned", &pset.popt, pset.quiet);
    }

    /* \i and \ir include files */
    else if (strcmp(cmd, "i") == 0 || strcmp(cmd, "i+") == 0 || strcmp(cmd, "include") == 0 ||
             strcmp(cmd, "include+") == 0 || strcmp(cmd, "ir") == 0 || strcmp(cmd, "ir+") == 0 ||
             strcmp(cmd, "include_relative") == 0 || strcmp(cmd, "include_relative+") == 0) {
        char* fname = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
        /* Database Security: Data importing/dumping support AES128. */
        tmplen = strlen(cmd);
        if ((tmplen > 1) && (strcmp(cmd + (tmplen - 1), "+") == 0)) {
            pset.decryptInfo.encryptInclude = true;
            dencrypt_key = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

            canAddHist = false;
            if (NULL != dencrypt_key) {
                tmpkeylen = strlen(dencrypt_key);

                isIllegal = check_input_password(dencrypt_key);
                if (isIllegal) {
                    rc = memset_s(pset.decryptInfo.Key, KEY_MAX_LEN, 0, KEY_MAX_LEN);
                    securec_check_c(rc, "\0", "\0");
                    rc = strncpy_s((char*)pset.decryptInfo.Key, KEY_MAX_LEN, dencrypt_key, KEY_MAX_LEN - 1);
                    securec_check_c(rc, dencrypt_key, "\0");
                }
                rc = memset_s(dencrypt_key, tmpkeylen, 0, tmpkeylen);
                securec_check_c(rc, dencrypt_key, "\0");
                free(dencrypt_key);
                dencrypt_key = NULL;
            }
        }

        if (NULL == fname) {
            psql_error("\\%s: missing required argument\n", cmd);
            success = false;
        } else {
            bool include_relative = false;

            include_relative = (strcmp(cmd, "ir") == 0 || strcmp(cmd, "ir+") == 0 ||
                                strcmp(cmd, "include_relative") == 0 || strcmp(cmd, "include_relative+") == 0);
            expand_tilde(&fname);
            /* Database Security: Data importing/dumping support AES128. */
            if ((pset.decryptInfo.encryptInclude == true && isIllegal) ||
                (pset.decryptInfo.encryptInclude == false)) {
                success = (process_file(fname, false, include_relative) == EXIT_SUCCESS);
            } else {
                psql_error("\\%s: Missing the key or the key is illegal,must be %d~%d bytes and "
                    "contain at least three kinds of characters!\n",
                    cmd,
                    MIN_KEY_LEN,
                    MAX_KEY_LEN);
            }
            free(fname);
            fname = NULL;
        }
        pset.decryptInfo.encryptInclude = false;
    }

    /* \l is list databases */
    else if (strcmp(cmd, "l") == 0 || strcmp(cmd, "list") == 0)
        success = listAllDbs(false);
    else if (strcmp(cmd, "l+") == 0 || strcmp(cmd, "list+") == 0)
        success = listAllDbs(true);

    /*
     * large object things
     */
    else if (strncmp(cmd, "lo_", 3) == 0) {
        char *opt1 = NULL, *opt2 = NULL;

        opt1 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
        opt2 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        if (strcmp(cmd + 3, "export") == 0) {
            if (NULL == opt2) {
                psql_error("\\%s: missing required argument\n", cmd);
                success = false;
            } else {
                expand_tilde(&opt2);
                success = do_lo_export(opt1, opt2);
            }
        }

        else if (strcmp(cmd + 3, "import") == 0) {
            if (NULL == opt1) {
                psql_error("\\%s: missing required argument\n", cmd);
                success = false;
            } else {
                expand_tilde(&opt1);
                success = do_lo_import(opt1, opt2);
            }
        }

        else if (strcmp(cmd + 3, "list") == 0)
            success = do_lo_list();

        else if (strcmp(cmd + 3, "unlink") == 0) {
            if (NULL == opt1) {
                psql_error("\\%s: missing required argument\n", cmd);
                success = false;
            } else
                success = do_lo_unlink(opt1);
        }

        else
            status = PSQL_CMD_UNKNOWN;

        free(opt1);
        opt1 = NULL;
        free(opt2);
        opt2 = NULL;
    }

    /* \o -- set query output */
    else if (strcmp(cmd, "o") == 0 || strcmp(cmd, "out") == 0) {
        char* fname = psql_scan_slash_option(scan_state, OT_FILEPIPE, NULL, true);

        expand_tilde(&fname);
        success = setQFout(fname);
        free(fname);
        fname = NULL;
    }

    /* \p prints the current query buffer */
    else if (strcmp(cmd, "p") == 0 || strcmp(cmd, "print") == 0) {
        if ((query_buf != NULL) && query_buf->len > 0)
            puts(query_buf->data);
        else if (!pset.quiet)
            puts(_("Query buffer is empty."));
        (void)fflush(stdout);
    }

    /* \prompt -- prompt and set variable */
    else if (strcmp(cmd, "prompt") == 0) {
        char *opt = NULL, *prompt_text = NULL;
        char *arg1 = NULL, *arg2 = NULL;

        arg1 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
        arg2 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == arg1) {
            psql_error("\\%s: missing required argument\n", cmd);
            success = false;
        } else {
            char* result = NULL;

            if (NULL != arg2) {
                prompt_text = arg1;
                opt = arg2;
            } else
                opt = arg1;

            if (NULL == pset.inputfile)
                result = simple_prompt(prompt_text, 4096, true);
            else {
                if (NULL != prompt_text) {
                    fputs(prompt_text, stdout);
                    (void)fflush(stdout);
                }
                result = gets_fromFile(stdin);
            }

            if (!SetVariable(pset.vars, opt, result)) {
                psql_error("\\%s: error while setting variable\n", cmd);
                success = false;
            }

            if (NULL != result)
                free(result);
            result = NULL;
            if (NULL != prompt_text)
                free(prompt_text);
            prompt_text = NULL;

            free(opt);
            opt = NULL;
        }
    }

    /* \pset -- set printing parameters */
    else if (strcmp(cmd, "pset") == 0) {
        char* opt0 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
        char* opt1 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == opt0) {
            psql_error("\\%s: missing required argument\n", cmd);
            success = false;
        } else
            success = do_pset(opt0, opt1, &pset.popt, pset.quiet);
        if (NULL != opt0)
            free(opt0);
        opt0 = NULL;
        if (NULL != opt1)
            free(opt1);
        opt1 = NULL;
    }

    /* \q or \quit */
    else if (strcmp(cmd, "q") == 0 || strcmp(cmd, "quit") == 0)
        status = PSQL_CMD_TERMINATE;

    /* reset(clear) the buffer */
    else if (strcmp(cmd, "r") == 0 || strcmp(cmd, "reset") == 0) {
        resetPQExpBuffer(query_buf);
        psql_scan_reset(scan_state);
        if (!pset.quiet)
            puts(_("Query buffer reset (cleared)."));
    }

    /* \set -- generalized set variable/option command */
    else if (strcmp(cmd, "set") == 0) {
        char* opt0 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == opt0) {
            /* list all variables */
            PrintVariables(pset.vars);
            success = true;
        } else {
            /*
             * Set variable to the concatenation of the arguments.
             */
            char* newval = NULL;
            char* opt = NULL;
            char* temp_opt = NULL;

            opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
            temp_opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
            newval = pg_strdup(opt != NULL ? opt : "");
            if (NULL != opt)
                free(opt);
            opt = NULL;

            /* \set RETRY need only one parameter. */
            if (strcmp(opt0, "RETRY") == 0 && (temp_opt != NULL)) {
                psql_error("Invalid retry times, need only one parameter.\n");
                if (NULL != newval) {
                    free(newval);
                    newval = NULL;
                }
                free(opt0);
                opt0 = NULL;
                free(temp_opt);
                temp_opt = NULL;
                return PSQL_CMD_ERROR;
            }

            while ((opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false)) != NULL) {
                // This value is to store the old value of newval.
                char* newval_old_ptr = newval;
                newval = (char*)malloc(strlen(newval_old_ptr) + strlen(opt) + 1);
                if (NULL == newval) {
                    psql_error("out of memory\n");
                    exit(EXIT_FAILURE);
                }
                check_sprintf_s(
                    sprintf_s(newval, strlen(newval_old_ptr) + strlen(opt) + 1, "%s%s", newval_old_ptr, opt));
                free(newval_old_ptr);
                newval_old_ptr = NULL;
                free(opt);
                opt = NULL;
            }

            if (!SetVariable(pset.vars, opt0, newval)) {
                psql_error("\\%s: error while setting variable\n", cmd);
                success = false;
            }

#ifdef USE_READLINE
            if (useReadline && pset.cur_cmd_interactive) {
                setHistSize(opt0, newval, false);
            }
#endif

            free(newval);
            newval = NULL;
            if (temp_opt != NULL)
                free(temp_opt);
            temp_opt = NULL;
        }
        if (opt0 != NULL)
            free(opt0);
        opt0 = NULL;
    }

    /* \setenv -- set environment command */
    else if (strcmp(cmd, "setenv") == 0) {
        char* envvar = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
        char* envval = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == envvar) {
            psql_error("\\%s: missing required argument\n", cmd);
            success = false;
        } else if (strchr(envvar, '=') != NULL) {
            psql_error("\\%s: environment variable name must not contain \"=\"\n", cmd);
            success = false;
        } else if (NULL == envval) {
            /* No argument - unset the environment variable */
            unsetenv(envvar);
            success = true;
        } else {
            /* Set variable to the value of the next argument */
            int len = strlen(envvar) + strlen(envval) + 1;
            char* newval = (char*)pg_malloc(len + 1);

            check_sprintf_s(sprintf_s(newval, len + 1, "%s=%s", envvar, envval));
            (void)putenv(newval);
            success = true;

            /*
             * Do not free newval here, it will screw up the environment if
             * you do. See putenv man page for details. That means we leak a
             * bit of memory here, but not enough to worry about.
             */
        }
        free(envvar);
        envvar = NULL;
        free(envval);
        envval = NULL;
    }

    /* \sf -- show a function's source code */
    else if (strcmp(cmd, "sf") == 0 || strcmp(cmd, "sf+") == 0) {
        bool show_linenumbers = (strcmp(cmd, "sf+") == 0);
        PQExpBuffer func_buf;
        char* func = NULL;
        Oid foid = InvalidOid;

        func_buf = createPQExpBuffer();
        func = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, true);
        if (pset.sversion < 80400) {
            psql_error("The server (version %d.%d) does not support showing function source.\n",
                pset.sversion / 10000,
                (pset.sversion / 100) % 100);
            status = PSQL_CMD_ERROR;
        } else if (NULL == func) {
            psql_error("function name is required\n");
            status = PSQL_CMD_ERROR;
        } else if (!lookup_function_oid(pset.db, func, &foid)) {
            /* error already reported */
            status = PSQL_CMD_ERROR;
        } else if (!get_create_function_cmd(pset.db, foid, func_buf)) {
            /* error already reported */
            status = PSQL_CMD_ERROR;
        } else {
            FILE* output = NULL;
            bool is_pager = false;

            /* Select output stream: stdout, pager, or file */
            if (pset.queryFout == stdout) {
                /* count lines in function to see if pager is needed */
                int lineno = 0;
                const char* lines = func_buf->data;

                while (*lines != '\0') {
                    lineno++;
                    /* find start of next line */
                    lines = strchr(lines, '\n');
                    if (NULL == lines)
                        break;
                    lines++;
                }

                output = PageOutput(lineno, pset.popt.topt.pager);
                is_pager = true;
            } else {
                /* use previously set output file, without pager */
                output = pset.queryFout;
                is_pager = false;
            }

            if (show_linenumbers) {
                bool in_header = true;
                int lineno = 0;
                char* lines = func_buf->data;

                /*
                 * lineno "1" should correspond to the first line of the
                 * function body.  We expect that pg_get_functiondef() will
                 * emit that on a line beginning with "AS ", and that there
                 * can be no such line before the real start of the function
                 * body.
                 *
                 * Note that this loop scribbles on func_buf.
                 */
                while (*lines != '\0') {
                    char* eol = NULL;

                    if (in_header && strncmp(lines, "AS ", 3) == 0) {
                        in_header = false;
                    }
                    /* increment lineno only for body's lines */
                    if (!in_header) {
                        lineno++;
                    }
                    /* find and mark end of current line */
                    eol = strchr(lines, '\n');
                    if (eol != NULL) {
                        *eol = '\0';
                    }
                    /* show current line as appropriate */
                    if (in_header) {
                        fprintf(output, "        %s\n", lines);
                    } else {
                        fprintf(output, "%-7d %s\n", lineno, lines);
                    }

                    /* advance to next line, if any */
                    if (eol == NULL) {
                        break;
                    }
                    lines = ++eol;
                }
            } else {
                /* just send the function definition to output */
                fputs(func_buf->data, output);
            }

            if (is_pager) {
                ClosePager(output);
            }
        }

        if (NULL != func) {
            free(func);
        }
        func = NULL;
        destroyPQExpBuffer(func_buf);
    }

    /* \t -- turn off headers and row count */
    else if (strcmp(cmd, "t") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        success = do_pset("tuples_only", opt, &pset.popt, pset.quiet);
        free(opt);
        opt = NULL;
    }

    /* \T -- define html <table ...> attributes */
    else if (strcmp(cmd, "T") == 0) {
        char* value = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        success = do_pset("tableattr", value, &pset.popt, pset.quiet);
        free(value);
        value = NULL;
    }

    /* \timing -- toggle timing of queries */
    else if (strcmp(cmd, "timing") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL != opt)
            pset.timing = ParseVariableBool(opt);
        else
            pset.timing = !pset.timing;
        if (!pset.quiet) {
            if (pset.timing)
                puts(_("Timing is on."));
            else
                puts(_("Timing is off."));
        }
        free(opt);
        opt = NULL;
    }

    /* \unset */
    else if (strcmp(cmd, "unset") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        if (NULL == opt) {
            psql_error("\\%s: missing required argument\n", cmd);
            success = false;
        } else if (!SetVariable(pset.vars, opt, NULL)) {
            psql_error("\\%s: error while setting variable\n", cmd);
            success = false;
        }

#ifdef USE_READLINE
        if (useReadline && pset.cur_cmd_interactive) {
            setHistSize(opt, NULL, true);
        }
#endif

        if (NULL != opt) {
            free(opt);
            opt = NULL;
        }
    }

    /* \w -- write query buffer to file */
    else if (strcmp(cmd, "w") == 0 || strcmp(cmd, "write") == 0) {
        FILE* fd = NULL;
        bool is_pipe = false;
        char* fname = NULL;

        if (query_buf == NULL) {
            psql_error("no query buffer\n");
            status = PSQL_CMD_ERROR;
        } else {
            fname = psql_scan_slash_option(scan_state, OT_FILEPIPE, NULL, true);
            expand_tilde(&fname);

            if (NULL == fname) {
                psql_error("\\%s: missing required argument\n", cmd);
                success = false;
            } else {
                if (fname[0] == '|') {
                    is_pipe = true;
                    fd = popen(&fname[1], "w");
                } else {
                    canonicalize_path(fname);
                    fd = fopen(fname, "w");
                }
                if (NULL == fd) {
                    psql_error("%s: %s\n", fname, strerror(errno));
                    success = false;
                }
            }
        }

        if (NULL != fd) {
            int result;

            if ((query_buf != NULL) && query_buf->len > 0)
                fprintf(fd, "%s\n", query_buf->data);

            if (is_pipe)
                result = pclose(fd);
            else
                result = fclose(fd);

            if (result == EOF) {
                psql_error("%s: %s\n", fname, strerror(errno));
                success = false;
            }
        }

        free(fname);
        fname = NULL;
    }

    /* \x -- set or toggle expanded table representation */
    else if (strcmp(cmd, "x") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        success = do_pset("expanded", opt, &pset.popt, pset.quiet);
        free(opt);
        opt = NULL;
    }

    /* \z -- list table rights (equivalent to \dp) */
    else if (strcmp(cmd, "z") == 0) {
        char* pattern = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        success = permissionsList(pattern);
        if (NULL != pattern) {
            free(pattern);
            pattern = NULL;
        }
    }

    /* \! -- shell escape */
    else if (strcmp(cmd, "!") == 0) {
        char* opt = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false);
        canAddHist = false;
        success = do_shell(opt);
        free(opt);
        opt = NULL;
    }

    /* \? -- slash command help */
    else if (strcmp(cmd, "?") == 0)
        slashUsage(pset.popt.topt.pager);

    /* \parallel -- parallel execute */
    else if (strcmp(cmd, "parallel") == 0) {
        PGTransactionStatusType transaction_status;
        char* opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
        char* num = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

        /* Don't support do parallel in transaction. */
        transaction_status = PQtransactionStatus(pset.db);
        if (transaction_status != PQTRANS_IDLE) {
            psql_error("Parallel within transaction is not supported\n");
            success = false;
        } else {
            /* Parse the parallel status and parallel num. */
            if (NULL != opt)
                pset.parallel = ParseVariableBool(opt);
            else {
                pset.parallel = !pset.parallel;
            }
            if (pset.parallel)
                pset.parallel_num = ParseVariableInt(num);

            if (!pset.quiet) {
                if (pset.parallel) {
                    /* The default maxium parallel exec number is MAX_STMTS. */
                    if (NULL == num) {
                        fprintf(stdout, "Parallel is on with scale default 1024.\n");
                    } else if (pset.parallel_num <= 0 || pset.parallel_num > MAX_STMTS) {
                        pset.parallel_num = 0;
                        pset.parallel = !pset.parallel;
                        psql_error("The valid parallel num is integer 1-%d.\n", MAX_STMTS);
                        success = false;
                    } else {
                        fprintf(stdout, "Parallel is on with scale %d.\n", pset.parallel_num);
                    }
                } else {
                    puts(_("Parallel is off."));
                }
            }
        }
        if (NULL != opt) {
            free(opt);
            opt = NULL;
        }
        if (NULL != num) {
            free(num);
            num = NULL;
        }
    } else
        status = PSQL_CMD_UNKNOWN;

    if (!success)
        status = PSQL_CMD_ERROR;

    return status;
}

/*
 * Ask the user for a password; 'username' is the username the
 * password is for, if one has been explicitly specified. Returns a
 * malloc'd string.
 */
static char* prompt_for_password(const char* username)
{
    char* result = NULL;

    if (username == NULL)
        result = simple_prompt("Password: ", MAX_PASSWORD_LENGTH, false);
    else {
        char* prompt_text = NULL;

        prompt_text = (char*)pg_malloc(strlen(username) + MAX_PASSWORD_LENGTH);
        check_sprintf_s(
            sprintf_s(prompt_text, strlen(username) + MAX_PASSWORD_LENGTH, _("Password for user %s: "), username));
        result = simple_prompt(prompt_text, MAX_PASSWORD_LENGTH, false);
        free(prompt_text);
        prompt_text = NULL;
    }

    return result;
}

static bool param_is_newly_set(const char* old_val, const char* new_val)
{
    if (new_val == NULL)
        return false;

    if (old_val == NULL || strcmp(old_val, new_val) != 0)
        return true;

    return false;
}

/*
 * do_connect -- handler for \connect
 *
 * Connects to a database with given parameters. If there exists an
 * established connection, NULL values will be replaced with the ones
 * in the current connection. Otherwise NULL will be passed for that
 * parameter to PQconnectdbParams(), so the libpq defaults will be used.
 *
 * In interactive mode, if connection fails with the given parameters,
 * the old connection will be kept.
 */
static bool do_connect(char* dbname, char* user, char* host, char* port)
{
    PGconn* o_conn = pset.db;
    PGconn* n_conn = NULL;
    char* password = NULL;
    bool isdbnamenull = false;
    bool isusernull = false;
    bool ishostnull = false;
    bool isportnull = false;
    errno_t rc;

    if (NULL == dbname) {
        dbname = PQdb(o_conn);
        isdbnamenull = true;
    }
    if (NULL == user) {
        user = PQuser(o_conn);
        isusernull = true;
    }
    if (NULL == host) {
        host = PQhost(o_conn);
        ishostnull = true;
    }
    if (NULL == port) {
        port = PQport(o_conn);
        isportnull = true;
    }

    while (true) {
#ifdef HAVE_CE
#define PARAMS_ARRAY_SIZE 10
#else
#define PARAMS_ARRAY_SIZE 9
#endif /* HAVE_CE */
        char* tmp = GetEnvStr("PGCLIENTENCODING");
        if (tmp != NULL) {
            check_env_value(tmp);
        }

        const char** keywords = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
        const char** values = (const char**)pg_malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

        keywords[0] = "host";
        values[0] = host;
        keywords[1] = "port";
        values[1] = port;
        keywords[2] = "user";
        values[2] = user;
        keywords[3] = "password";
        values[3] = password;
        keywords[4] = "dbname";
        values[4] = dbname;
        keywords[5] = "fallback_application_name";
        values[5] = pset.progname;
        keywords[6] = "client_encoding";
        values[6] = (pset.notty || (tmp != NULL)) ? NULL : "auto";
        keywords[7] = "connect_timeout";
        values[7] = CONNECT_TIMEOUT;
#ifdef HAVE_CE
        keywords[8] = "enable_ce";
        values[8] = (pset.enable_client_encryption) ? "1" : NULL;
#endif
        keywords[PARAMS_ARRAY_SIZE-1] = NULL;
        values[PARAMS_ARRAY_SIZE-1] = NULL;

        n_conn = PQconnectdbParams(keywords, values, true);

        free(keywords);
        keywords = NULL;
        rc = memset_s(values, PARAMS_ARRAY_SIZE * sizeof(*values), 0, PARAMS_ARRAY_SIZE * sizeof(*values));
        check_memset_s(rc);
        free(values);
        values = NULL;
        if (NULL != tmp)
            free(tmp);
        tmp = NULL;

        if (PQstatus(n_conn) == CONNECTION_OK) {
            /* Update the connection-dependent variables for parallel execution. */
            if (!ishostnull) {
                if (pset.connInfo.values_free[0] && (pset.connInfo.values[0] != NULL))
                    free(pset.connInfo.values[0]);
                pset.connInfo.values[0] = pg_strdup(host);
                pset.connInfo.values_free[0] = true;
            }
            if (!isportnull) {
                if (pset.connInfo.values_free[1] && (pset.connInfo.values[1] != NULL))
                    free(pset.connInfo.values[1]);
                pset.connInfo.values[1] = pg_strdup(port);
                pset.connInfo.values_free[1] = true;
            }
            if (!isusernull) {
                if (pset.connInfo.values_free[2] && (pset.connInfo.values[2] != NULL))
                    free(pset.connInfo.values[2]);
                pset.connInfo.values[2] = pg_strdup(user);
                pset.connInfo.values_free[2] = true;
            }
            if (!isdbnamenull) {
                if (pset.connInfo.values_free[4] && (pset.connInfo.values[4] != NULL))
                    free(pset.connInfo.values[4]);
                pset.connInfo.values[4] = pg_strdup(dbname);
                pset.connInfo.values_free[4] = true;
            }
            if (NULL != password && strlen(password) != 0) {
                if (pset.connInfo.values_free[3] && (pset.connInfo.values[3] != NULL)) {
                    free(pset.connInfo.values[3]);
                    pset.connInfo.values[3] = NULL;
                }
                pset.connInfo.values[3] = SEC_encodeBase64((char*)password, strlen(password));
                if (pset.connInfo.values[3] == NULL) {
                    fprintf(stderr, "%s: encode the parallel connect value failed.", pset.progname);
                    exit(EXIT_BADCONN);
                }
                pset.connInfo.values_free[3] = true;
            }
            /* Clear the memory of sensitive data like password. */
            clear_sensitive_memory(password, TRUE);
            password = NULL;
            clear_sensitive_memory(PQpass(n_conn), FALSE);
            break;
        }

        /*
         * Connection attempt failed; either retry the connection attempt with
         * a new password, or give up.
         */
        if (NULL == password && (strstr(PQerrorMessage(n_conn), "password") != NULL) && pset.getPassword != TRI_NO) {
            /* Get latest user name from current connection */
            password = prompt_for_password(PQuser(n_conn));
            PQfinish(n_conn);
            continue;
        }

        /* Clear the memory of sensitive data like password. */
        clear_sensitive_memory(password, TRUE);
        password = NULL;
        clear_sensitive_memory(PQpass(n_conn), FALSE);

        /*
         * Failed to connect to the database. In interactive mode, keep the
         * previous connection to the DB; in scripting mode, close our
         * previous connection as well.
         */
        if (pset.cur_cmd_interactive) {
            psql_error("%s", PQerrorMessage(n_conn));

            /* pset.db is left unmodified */
            if (NULL != o_conn)
                fputs(_("Previous connection kept\n"), stderr);
        } else {
            psql_error("\\connect: %s", PQerrorMessage(n_conn));
            if (NULL != o_conn) {
                PQfinish(o_conn);
                pset.db = NULL;
            }
        }

        PQfinish(n_conn);
        return false;
    }

    /*
     * Replace the old connection with the new one, and update
     * connection-dependent variables.
     */
    (void)PQsetNoticeProcessor(n_conn, NoticeProcessor, NULL);
    pset.db = n_conn;
    SyncVariables();
    connection_warnings(false); /* Must be after SyncVariables */

    /* Tell the user about the new connection */
    if (!pset.quiet) {
        if (param_is_newly_set(PQhost(o_conn), PQhost(pset.db)) ||
            param_is_newly_set(PQport(o_conn), PQport(pset.db))) {
            char* host = PQhost(pset.db);

            if (host == NULL)
                host = DEFAULT_PGSOCKET_DIR;
            /* If the host is an absolute path, the connection is via socket */
            if (is_absolute_path(host))
                printf(
                    _("You are now connected to database \"%s\" as user \"%s\" via socket in \"%s\" at port \"%s\".\n"),
                    PQdb(pset.db),
                    PQuser(pset.db),
                    host,
                    PQport(pset.db));
            else
                printf(_("You are now connected to database \"%s\" as user \"%s\" on host \"%s\" at port \"%s\".\n"),
                    PQdb(pset.db),
                    PQuser(pset.db),
                    host,
                    PQport(pset.db));
        } else
            printf(_("You are now connected to database \"%s\" as user \"%s\".\n"), PQdb(pset.db), PQuser(pset.db));
    }

    if (NULL != o_conn)
        PQfinish(o_conn);
    return true;
}

void connection_warnings(bool in_startup)
{
    if (!pset.quiet && !pset.notty) {
        int client_ver = parse_version(PG_VERSION);
        if (pset.sversion != client_ver) {
            const char* server_version = NULL;
            char server_ver_str[16];

            /* Try to get full text form, might include "devel" etc */
            server_version = PQparameterStatus(pset.db, "server_version");
            if (NULL == server_version) {
                check_sprintf_s(sprintf_s(server_ver_str,
                    sizeof(server_ver_str),
                    "%d.%d.%d",
                    pset.sversion / 10000,
                    (pset.sversion / 100) % 100,
                    pset.sversion % 100));
                server_version = server_ver_str;
            }

            printf(_("%s (%s, server %s)\n"), pset.progname, PG_VERSION, server_version);
        }
        /* For version match, only print psql banner on startup. */
        else if (in_startup) {
#ifdef PGXC
            printf("%s (%s)\n", pset.progname, DEF_GS_VERSION);
#else
            printf("%s (%s)\n", pset.progname, PG_VERSION);
#endif
        }

#ifndef ENABLE_LITE_MODE
        /* show notice message when the password expired time will come. */
        if (in_startup)
            (void)show_password_notify(pset.db);
#endif

        if (pset.sversion / 100 != client_ver / 100)
            printf(_("WARNING: %s version %d.%d, server version %d.%d.\n"
                     "         Some gsql features might not work.\n"),
                pset.progname,
                client_ver / 10000,
                (client_ver / 100) % 100,
                pset.sversion / 10000,
                (pset.sversion / 100) % 100);

#ifdef WIN32
        checkWin32Codepage();
#endif
        printSSLInfo();
    }
}

/*
 * printSSLInfo
 *
 * Prints information about the current SSL connection, if SSL is in use
 */
static void printSSLInfo(void)
{
#ifdef USE_SSL
    int sslbits = -1;
    SSL* ssl = NULL;

    ssl = (SSL*)PQgetssl(pset.db);
    if (NULL == ssl) {
        printf(_("Non-SSL connection (SSL connection is recommended when requiring high-security)\n"));
        return; /* no SSL */
    }

    SSL_CIPHER_get_bits(SSL_get_current_cipher(ssl), &sslbits);

    printf(("SSL connection (cipher: %s, bits: %d)\n"), SSL_CIPHER_get_name(SSL_get_current_cipher(ssl)), sslbits);
#else

    /*
     * If psql is compiled without SSL but is using a libpq with SSL, we
     * cannot figure out the specifics about the connection. But we know it's
     * SSL secured.
     */
    if (PQgetssl(pset.db))
        printf(_("SSL connection (unknown cipher)\n"));
#endif
}

/*
 * checkWin32Codepage
 *
 * Prints a warning when win32 console codepage differs from Windows codepage
 */
#ifdef WIN32
static void checkWin32Codepage(void)
{
    unsigned int wincp, concp;

    wincp = GetACP();
    concp = GetConsoleCP();
    if (wincp != concp) {
        printf(_("WARNING: Console code page (%u) differs from Windows code page (%u)\n"
                 "         8-bit characters might not work correctly. See gsql reference\n"
                 "         page \"Notes for Windows users\" for details.\n"),
            concp,
            wincp);
    }
}
#endif

/*
 * SyncVariables
 *
 * Make psql's internal variables agree with connection state upon
 * establishing a new connection.
 */
void SyncVariables(void)
{
    /* get stuff from connection */
    pset.encoding = PQclientEncoding(pset.db);
    pset.popt.topt.encoding = pset.encoding;
    pset.sversion = PQserverVersion(pset.db);

    if (!SetVariable(pset.vars, "DBNAME", PQdb(pset.db))) {
        psql_error("set variable %s failed.\n", "DBNAME");
    }
    if (!SetVariable(pset.vars, "USER", PQuser(pset.db))) {
        psql_error("set variable %s failed.\n", "USER");
    }
    if (!SetVariable(pset.vars, "HOST", PQhost(pset.db))) {
        psql_error("set variable %s failed.\n", "HOST");
    }
    if (!SetVariable(pset.vars, "PORT", PQport(pset.db))) {
        psql_error("set variable %s failed.\n", "PORT");
    }
    if (!SetVariable(pset.vars, "ENCODING", pg_encoding_to_char(pset.encoding))) {
        psql_error("set variable %s failed.\n", "ENCODING");
    }

    /* send stuff to it, too */
    (void)PQsetErrorVerbosity(pset.db, pset.verbosity);
}

/*
 * UnsyncVariables
 *
 * Clear variables that should be not be set when there is no connection.
 */
void UnsyncVariables(void)
{
    if (!SetVariable(pset.vars, "DBNAME", NULL)) {
        psql_error("set variable %s failed.\n", "DBNAME");
    }
    if (!SetVariable(pset.vars, "USER", NULL)) {
        psql_error("set variable %s failed.\n", "USER");
    }
    if (!SetVariable(pset.vars, "HOST", NULL)) {
        psql_error("set variable %s failed.\n", "HOST");
    }
    if (!SetVariable(pset.vars, "PORT", NULL)) {
        psql_error("set variable %s failed.\n", "PORT");
    }
    if (!SetVariable(pset.vars, "ENCODING", NULL)) {
        psql_error("set variable %s failed.\n", "ENCODING");
    }
}

/*
 * do_edit -- handler for \e
 *
 * If you do not specify a filename, the current query buffer will be copied
 * into a temporary one.
 */
static bool editFile(const char* fname, int lineno)
{
    char* editorName = NULL;
    char* editor_lineno_arg = NULL;
    char* sys = NULL;
    size_t syssz = 0;
    int result = 0;

    psql_assert(fname);

    /* Find an editor to use */
    editorName = GetEnvStr("PSQL_EDITOR");

    if (NULL == editorName)
        editorName = GetEnvStr("EDITOR");
    if (NULL == editorName)
        editorName = GetEnvStr("VISUAL");

    if (NULL == editorName) {
        editorName = pg_strdup(DEFAULT_EDITOR);
    } else {
        check_env_value(editorName);
    }

    if (strlen(editorName) >= MAXPGPATH) {
        psql_error("The value of \"editorName\" is too long.\n");
        free(editorName);
        editorName = NULL;
        return false;
    }

    /* Get line number argument, if we need it. */
    if (lineno > 0) {
        editor_lineno_arg = GetEnvStr("PSQL_EDITOR_LINENUMBER_ARG");
#ifdef DEFAULT_EDITOR_LINENUMBER_ARG
        if (NULL == editor_lineno_arg) {
            editor_lineno_arg = pg_strdup(DEFAULT_EDITOR_LINENUMBER_ARG);
        }
#endif
        if (NULL == editor_lineno_arg) {
            psql_error("environment variable PSQL_EDITOR_LINENUMBER_ARG must be set to specify a line number\n");
            free(editorName);
            editorName = NULL;
            return false;
        }
        check_env_value(editor_lineno_arg);

        if (strlen(editor_lineno_arg) >= MAXPGPATH) {
            psql_error("The value of \"editor_lineno_arg\" is too long.\n");
            free(editor_lineno_arg);
            editor_lineno_arg = NULL;
            free(editorName);
            editorName = NULL;
            return false;
        }

        syssz = strlen(editorName) + strlen(editor_lineno_arg) + 10 /* for integer */
                 + 1 + strlen(fname) + 10 + 1;
    } else {
        syssz = strlen(editorName) + strlen(fname) + 10 + 1;
    }

    /* Allocate sufficient memory for command line. */
    sys = (char*)pg_malloc(syssz);

    /*
     * On Unix the EDITOR value should *not* be quoted, since it might include
     * switches, eg, EDITOR="pico -t"; it's up to the user to put quotes in it
     * if necessary.  But this policy is not very workable on Windows, due to
     * severe brain damage in their command shell plus the fact that standard
     * program paths include spaces.
     */
#ifndef WIN32
    if (lineno > 0) {
        check_sprintf_s(sprintf_s(sys, syssz, "exec %s %s%d '%s'", editorName, editor_lineno_arg, lineno, fname));
    } else {
        check_sprintf_s(sprintf_s(sys, syssz, "exec %s '%s'", editorName, fname));
    }
#else
    if (lineno > 0) {
        check_sprintf_s(sprintf_s(
            sys, syssz, SYSTEMQUOTE "\"%s\" %s%d \"%s\"" SYSTEMQUOTE, editorName, editor_lineno_arg, lineno, fname));
    } else {
        check_sprintf_s(sprintf_s(sys, syssz, SYSTEMQUOTE "\"%s\" \"%s\"" SYSTEMQUOTE, editorName, fname));
    }
#endif
    result = system(sys);
    if (result == -1) {
        psql_error("could not start editor \"%s\"\n", editorName);
    } else if (result == 127) {
        psql_error("could not start /bin/sh\n");
    }
    free(sys);
    sys = NULL;

    if (editorName != NULL) {
        free(editorName);
    }
    editorName = NULL;
    if (editor_lineno_arg != NULL) {
        free(editor_lineno_arg);
    }
    editor_lineno_arg = NULL;
    return result == 0;
}

/* call this one */
static bool do_edit(const char* filename_arg, PQExpBuffer query_buf, int lineno, bool* edited)
{
    char fnametmp[MAXPGPATH];
    FILE* stream = NULL;
    char* fname = NULL;
    bool error = false;
    int fd = 0;
    bool needfreetmpdir = true;

    struct stat before, after;

    if (NULL != filename_arg) {
        errno_t err = EOK;
        err = strcpy_s(fnametmp, sizeof(fnametmp), filename_arg);
        check_strcpy_s(err);
        fname = fnametmp;
    } else {
        /* make a temp file to edit */
#ifndef WIN32
        char* tmpdir = GetEnvStr("TMPDIR");
        if (tmpdir != NULL) {
            check_env_value(tmpdir);
        }

        if (NULL == tmpdir) {
            needfreetmpdir = false;
            tmpdir = "/tmp";
        }

#else
        char tmpdir[MAXPGPATH];
        int ret;

        ret = GetTempPath(MAXPGPATH, tmpdir);
        if (ret == 0 || ret > MAXPGPATH) {
            psql_error("could not locate temporary directory: %s\n", !ret ? strerror(errno) : "");
            return false;
        }

        /*
         * No canonicalize_path() here. EDIT.EXE run from CMD.EXE prepends the
         * current directory to the supplied path unless we use only
         * backslashes, so we do that.
         */
#endif
#ifndef WIN32
        check_sprintf_s(sprintf_s(fnametmp, sizeof(fnametmp), "%s%spsql.edit.%d.sql", tmpdir, "/", (int)getpid()));
#else
        check_sprintf_s(sprintf_s(fnametmp,
            sizeof(fnametmp),
            "%s%spsql.edit.%d.sql",
            tmpdir,
            "" /* trailing separator already present */,
            (int)getpid()));
#endif

        fname = (char*)fnametmp;

        fd = open(fname, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd != -1)
            stream = fdopen(fd, "w");

        if (fd == -1 || NULL == stream) {
            psql_error("could not open temporary file \"%s\": %s\n", fname, strerror(errno));
            error = true;
        } else {
            unsigned int ql = query_buf->len;

            if (ql == 0 || query_buf->data[ql - 1] != '\n') {
                appendPQExpBufferChar(query_buf, '\n');
                ql++;
            }

            if (fwrite(query_buf->data, 1, ql, stream) != ql) {
                psql_error("%s: %s\n", fname, strerror(errno));
                fclose(stream);
                remove(fname);
                error = true;
            } else if (fclose(stream) != 0) {
                psql_error("%s: %s\n", fname, strerror(errno));
                remove(fname);
                error = true;
            }
        }
#ifndef WIN32
        if (needfreetmpdir) {
            free(tmpdir);
            tmpdir = NULL;
        }
#endif
    }

    if (!error && stat(fname, &before) != 0) {
        psql_error("%s: %s\n", fname, strerror(errno));
        error = true;
    }

    /* call editor */
    if (!error) {
        error = !editFile(fname, lineno);
    }
    if (!error && stat(fname, &after) != 0) {
        psql_error("%s: %s\n", fname, strerror(errno));
        error = true;
    }

    if (!error && before.st_mtime != after.st_mtime) {
        canonicalize_path(fname);
        stream = fopen(fname, PG_BINARY_R);
        if (NULL == stream) {
            psql_error("%s: %s\n", fname, strerror(errno));
            error = true;
        } else {
            /* read file back into query_buf */
            char line[1024];

            resetPQExpBuffer(query_buf);
            while (fgets(line, sizeof(line), stream) != NULL)
                appendPQExpBufferStr(query_buf, line);

            if (ferror(stream)) {
                psql_error("%s: %s\n", fname, strerror(errno));
                error = true;
            } else if (NULL != edited) {
                *edited = true;
            }

            fclose(stream);
        }
    }

    /* remove temp file */
    if (NULL == filename_arg) {
        if (remove(fname) == -1) {
            psql_error("%s: %s\n", fname, strerror(errno));
            error = true;
        }
    }

    return !error;
}

int ProcessFileInternal(char **fileName, bool useRelativePath, char *relPath, int relPathSize, FILE **fd)
{
    errno_t rc = EOK;
    if (strcmp(*fileName, "-") != 0) {
        canonicalize_path(*fileName);

        /*
         * If we were asked to resolve the pathname relative to the location
         * of the currently executing script, and there is one, and this is a
         * relative pathname, then prepend all but the last pathname component
         * of the current script to this pathname.
         */
        if (useRelativePath && (pset.inputfile != NULL) &&
            !is_absolute_path(*fileName) && !has_drive_prefix(*fileName)) {
            rc = strcpy_s(relPath, relPathSize, pset.inputfile);
            check_strcpy_s(rc);
            get_parent_directory(relPath);
            join_path_components(relPath, relPath, *fileName);
            canonicalize_path(relPath);
            *fileName = relPath;
        }

        *fd = fopen(*fileName, PG_BINARY_R);

        if (*fd == NULL) {
            psql_error("%s: %s\n", *fileName, strerror(errno));
            return EXIT_FAILURE;
        }
    } else {
        *fd = stdin;
        *fileName = "<stdin>"; /* for future error messages */
    }

    return EXIT_SUCCESS;
}
/*
 * process_file
 *
 * Read commands from filename and then them to the main processing loop
 * Handler for \i and \ir, but can be used for other things as well.  Returns
 * MainLoop() error code.
 *
 * If use_relative_path is true and filename is not an absolute path, then open
 * the file from where the currently processed file (if any) is located.
 */
int process_file(char* filename, bool single_txn, bool use_relative_path)
{
    FILE* fd = NULL;
    int result;
    char* oldfilename = NULL;
    char relpath[MAXPGPATH] = {0};
    PGresult* res = NULL;

    if (NULL == filename)
        return EXIT_FAILURE;

    if (ProcessFileInternal(&filename, use_relative_path, relpath, sizeof(relpath), &fd) == EXIT_FAILURE) {
        return EXIT_FAILURE;
    }

    oldfilename = pset.inputfile;
    pset.inputfile = filename;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    if (pset.parseonly)
        single_txn = false;
#endif
    if (single_txn) {
        if ((res = PSQLexec("START TRANSACTION", false)) == NULL) {
            if (pset.on_error_stop) {
                result = EXIT_USER;
                goto error;
            }
        } else
            PQclear(res);
    }

    result = MainLoop(fd);

    if (single_txn) {
        if ((res = PSQLexec("COMMIT", false)) == NULL) {
            if (pset.on_error_stop) {
                result = EXIT_USER;
                goto error;
            }
        } else
            PQclear(res);
    }

error:
    if (fd != stdin)
        fclose(fd);

    /*
     *Database Security: Data importing/dumping support AES128.
     *If we are process gsqlrc file ,we won't clear the key info.
     */
    if (single_txn || use_relative_path)
        initDecryptInfo(&pset.decryptInfo);

    pset.inputfile = oldfilename;
    return result;
}

/*
 * do_pset
 *
 */
static const char* _align2string(enum printFormat in)
{
    switch (in) {
        case PRINT_NOTHING:
            return "nothing";
        case PRINT_UNALIGNED:
            return "unaligned";
        case PRINT_ALIGNED:
            return "aligned";
        case PRINT_WRAPPED:
            return "wrapped";
        case PRINT_HTML:
            return "html";
        case PRINT_LATEX:
            return "latex";
        case PRINT_TROFF_MS:
            return "troff-ms";
        default:
            break;
    }
    return "unknown";
}

static bool setToptFormat(const char* value, size_t vallen, printQueryOpt* popt, bool quiet)
{
    if (value == NULL)
        ;
    else if (pg_strncasecmp("unaligned", value, vallen) == 0)
        popt->topt.format = PRINT_UNALIGNED;
    else if (pg_strncasecmp("aligned", value, vallen) == 0)
        popt->topt.format = PRINT_ALIGNED;
    else if (pg_strncasecmp("wrapped", value, vallen) == 0)
        popt->topt.format = PRINT_WRAPPED;
    else if (pg_strncasecmp("html", value, vallen) == 0)
        popt->topt.format = PRINT_HTML;
    else if (pg_strncasecmp("latex", value, vallen) == 0)
        popt->topt.format = PRINT_LATEX;
    else if (pg_strncasecmp("troff-ms", value, vallen) == 0)
        popt->topt.format = PRINT_TROFF_MS;
    else {
        psql_error("\\pset: allowed formats are unaligned, aligned, wrapped, html, latex, troff-ms\n");
        return false;
    }

    if (!quiet)
        printf(_("Output format is %s.\n"), _align2string(popt->topt.format));

    return true;
}

static bool setFeedBack(const char* value, printQueryOpt* popt, bool quiet)
{
    if (value != NULL) {
        popt->topt.feedback = ParseVariableBool(value);
    } else {
        popt->topt.feedback = !popt->topt.feedback;
    }

    if (!quiet) {
        if (popt->topt.feedback) {
            puts(_("Showing rows count feedback."));
        } else {
            puts(_("Rows count feedback is off."));
        }
    }

    return true;
}

bool do_pset(const char* param, const char* value, printQueryOpt* popt, bool quiet)
{
    size_t vallen = 0;

    psql_assert(param);

    if (NULL != value)
        vallen = strlen(value);

    /* set format */
    if (strcmp(param, "format") == 0) {
        if(!setToptFormat(value, vallen, popt, quiet)) {
            return false;
        }
    }

    /* set feedback rows  */
    else if (strcmp(param, "feedback") == 0) {
        if(!setFeedBack(value, popt, quiet)) {
            return false;
        }
    }

    /* set table line style */
    else if (strcmp(param, "linestyle") == 0) {
        if (NULL == value)
            ;
        else if (pg_strncasecmp("ascii", value, vallen) == 0)
            popt->topt.line_style = &pg_asciiformat;
        else if (pg_strncasecmp("old-ascii", value, vallen) == 0)
            popt->topt.line_style = &pg_asciiformat_old;
        else if (pg_strncasecmp("unicode", value, vallen) == 0)
            popt->topt.line_style = &pg_utf8format;
        else {
            psql_error("\\pset: allowed line styles are ascii, old-ascii, unicode\n");
            return false;
        }

        if (!quiet)
            printf(_("Line style is %s.\n"), get_line_style(&popt->topt)->name);
    }

    /* set border style/width */
    else if (strcmp(param, "border") == 0) {
        if (NULL != value)
            popt->topt.border = atoi(value);

        if (!quiet)
            printf(_("Border style is %d.\n"), popt->topt.border);
    }

    /* set expanded/vertical mode */
    else if (strcmp(param, "x") == 0 || strcmp(param, "expanded") == 0 || strcmp(param, "vertical") == 0) {
        if (NULL != value && pg_strcasecmp(value, "auto") == 0)
            popt->topt.expanded = 2;
        else if (NULL != value)
            popt->topt.expanded = ParseVariableBool(value);
        else
            popt->topt.expanded = !popt->topt.expanded;
        if (!quiet) {
            if (popt->topt.expanded == 1)
                printf(_("Expanded display is on.\n"));
            else if (popt->topt.expanded == 2)
                printf(_("Expanded display is used automatically.\n"));
            else
                printf(_("Expanded display is off.\n"));
        }
    }

    /* locale-aware numeric output */
    else if (strcmp(param, "numericlocale") == 0) {
        if (NULL != value)
            popt->topt.numericLocale = ParseVariableBool(value);
        else
            popt->topt.numericLocale = !popt->topt.numericLocale;
        if (!quiet) {
            if (popt->topt.numericLocale)
                puts(_("Showing locale-adjusted numeric output."));
            else
                puts(_("Locale-adjusted numeric output is off."));
        }
    }

    /* null display */
    else if (strcmp(param, "null") == 0) {
        if (NULL != value) {
            free(popt->nullPrint);
            popt->nullPrint = pg_strdup(value);
        }
        if (!quiet)
            printf(_("Null display is \"%s\".\n"), popt->nullPrint != NULL ? popt->nullPrint : "");
    }

    /* field separator for unaligned text */
    else if (strcmp(param, "fieldsep") == 0) {
        if (NULL != value) {
            free(popt->topt.fieldSep.separator);
            popt->topt.fieldSep.separator = pg_strdup(value);
            popt->topt.fieldSep.separator_zero = false;
        }
        if (!quiet) {
            if (popt->topt.fieldSep.separator_zero)
                printf(_("Field separator is zero byte.\n"));
            else
                printf(_("Field separator is \"%s\".\n"), popt->topt.fieldSep.separator);
        }
    }

    else if (strcmp(param, "fieldsep_zero") == 0) {
        free(popt->topt.fieldSep.separator);
        popt->topt.fieldSep.separator = NULL;
        popt->topt.fieldSep.separator_zero = true;
        if (!quiet)
            printf(_("Field separator is zero byte.\n"));
    }

    /* record separator for unaligned text */
    else if (strcmp(param, "recordsep") == 0) {
        if (NULL != value) {
            free(popt->topt.recordSep.separator);
            popt->topt.recordSep.separator = pg_strdup(value);
            popt->topt.recordSep.separator_zero = false;
        }
        if (!quiet) {
            if (popt->topt.recordSep.separator_zero)
                printf(_("Record separator is zero byte.\n"));
            else if (strcmp(popt->topt.recordSep.separator, "\n") == 0)
                printf(_("Record separator is <newline>."));
            else
                printf(_("Record separator is \"%s\".\n"), popt->topt.recordSep.separator);
        }
    }

    else if (strcmp(param, "recordsep_zero") == 0) {
        free(popt->topt.recordSep.separator);
        popt->topt.recordSep.separator = NULL;
        popt->topt.recordSep.separator_zero = true;
        if (!quiet)
            printf(_("Record separator is zero byte.\n"));
    }

    /* toggle between full and tuples-only format */
    else if (strcmp(param, "t") == 0 || strcmp(param, "tuples_only") == 0) {
        if (NULL != value)
            popt->topt.tuples_only = ParseVariableBool(value);
        else
            popt->topt.tuples_only = !popt->topt.tuples_only;
        if (!quiet) {
            if (popt->topt.tuples_only)
                puts(_("Showing only tuples."));
            else
                puts(_("Tuples only is off."));
        }
    }

    /* set title override */
    else if (strcmp(param, "title") == 0) {
        free(popt->title);
        if (NULL == value)
            popt->title = NULL;
        else
            popt->title = pg_strdup(value);

        if (!quiet) {
            if (NULL != popt->title)
                printf(_("Title is \"%s\".\n"), popt->title);
            else
                printf(_("Title is unset.\n"));
        }
    }

    /* set HTML table tag options */
    else if (strcmp(param, "T") == 0 || strcmp(param, "tableattr") == 0) {
        free(popt->topt.tableAttr);
        if (NULL == value)
            popt->topt.tableAttr = NULL;
        else
            popt->topt.tableAttr = pg_strdup(value);

        if (!quiet) {
            if (popt->topt.tableAttr != NULL)
                printf(_("Table attribute is \"%s\".\n"), popt->topt.tableAttr);
            else
                printf(_("Table attributes unset.\n"));
        }
    }

    /* toggle use of pager */
    else if (strcmp(param, "pager") == 0) {
        if (NULL != value && pg_strcasecmp(value, "always") == 0) {
            popt->topt.pager = 2;
        }
        else if (NULL != value)
            if (ParseVariableBool(value))
                popt->topt.pager = 1;
            else
                popt->topt.pager = 0;
        else if (popt->topt.pager == 1)
            popt->topt.pager = 0;
        else
            popt->topt.pager = 1;
        if (!quiet) {
            if (popt->topt.pager == 1)
                puts(_("Pager is used for long output."));
            else if (popt->topt.pager == 2)
                puts(_("Pager is always used."));
            else
                puts(_("Pager usage is off."));
        }
    }

    /* disable "(x rows)" footer */
    else if (strcmp(param, "footer") == 0) {
        if (NULL != value)
            popt->topt.default_footer = ParseVariableBool(value);
        else
            popt->topt.default_footer = !popt->topt.default_footer;
        if (!quiet) {
            if (popt->topt.default_footer)
                puts(_("Default footer is on."));
            else
                puts(_("Default footer is off."));
        }
    }

    /* set border style/width */
    else if (strcmp(param, "columns") == 0) {
        if (NULL != value)
            popt->topt.columns = atoi(value);

        if (!quiet)
            printf(_("Target width is %d.\n"), popt->topt.columns);
    }

    else {
        psql_error("\\pset: unknown option: %s\n", param);
        return false;
    }

    return true;
}

#ifndef WIN32
#define DEFAULT_SHELL "/bin/sh"
#else
/*
 *	CMD.EXE is in different places in different Win32 releases so we
 *	have to rely on the path to find it.
 */
#define DEFAULT_SHELL "cmd.exe"
#endif

static bool do_shell(const char* command)
{
    int result;

    if (NULL == command) {
        char* sys = NULL;
        char* shellName = NULL;
        bool b_default_shellname = false;

        shellName = GetEnvStr("SHELL");
#ifdef WIN32
        if (shellName == NULL)
            shellName = GetEnvStr("COMSPEC");
#endif
        if (shellName == NULL) {
            shellName = DEFAULT_SHELL;
            b_default_shellname = true;
        } else {
            check_env_value(shellName);
        }

        if (strlen(shellName) >= MAXPGPATH) {
            psql_error("The value of \"SHELL\" is too long.\n");
            free(shellName);
            shellName = NULL;
            return false;
        }

        sys = (char*)pg_malloc(strlen(shellName) + 16);
#ifndef WIN32
        check_sprintf_s(sprintf_s(sys,
            strlen(shellName) + 16,
            /* See EDITOR handling comment for an explanation */
            "exec %s",
            shellName));
#else
        /* See EDITOR handling comment for an explanation */
        check_sprintf_s(sprintf_s(sys, strlen(shellName) + 16, SYSTEMQUOTE "\"%s\"" SYSTEMQUOTE, shellName));
#endif
        result = system(sys);
        free(sys);
        sys = NULL;

        if (!b_default_shellname)
            free(shellName);
        shellName = NULL;
    } else
        result = system(command);

    if (result == 127 || result == -1) {
        psql_error("\\!: failed\n");
        return false;
    }
    return true;
}

/*
 * This function takes a function description, e.g. "x" or "x(int)", and
 * issues a query on the given connection to retrieve the function's OID
 * using a cast to regproc or regprocedure (as appropriate). The result,
 * if there is one, is returned at *foid.  Note that we'll fail if the
 * function doesn't exist OR if there are multiple matching candidates
 * OR if there's something syntactically wrong with the function description;
 * unfortunately it can be hard to tell the difference.
 */
static bool lookup_function_oid(PGconn* conn, const char* desc, Oid* foid)
{
    bool result = true;
    PQExpBuffer query;
    PGresult* res = NULL;

    query = createPQExpBuffer();
    printfPQExpBuffer(query, "SELECT ");
    appendStringLiteralConn(query, desc, conn);
    appendPQExpBuffer(query, "::pg_catalog.%s::pg_catalog.oid", strchr(desc, '(') != NULL ? "regprocedure" : "regproc");

    res = PQexec(conn, query->data);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        *foid = atooid(PQgetvalue(res, 0, 0));
    else {
        minimal_error_message(res);
        result = false;
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return result;
}

/*
 * Fetches the "CREATE OR REPLACE FUNCTION ..." command that describes the
 * function with the given OID.  If successful, the result is stored in buf.
 */
static bool get_create_function_cmd(PGconn* conn, Oid oid, PQExpBuffer buf)
{
    bool result = true;
    PQExpBuffer query = NULL;
    PGresult* res = NULL;

    query = createPQExpBuffer();
    printfPQExpBuffer(query, "SELECT * FROM pg_catalog.pg_get_functiondef(%u)", oid);

    res = PQexec(conn, query->data);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        resetPQExpBuffer(buf);
        appendPQExpBufferStr(buf, PQgetvalue(res, 0, 1));
    } else {
        minimal_error_message(res);
        result = false;
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return result;
}

/*
 * If the given argument of \ef ends with a line number, delete the line
 * number from the argument string and return it as an integer.  (We need
 * this kluge because we're too lazy to parse \ef's function name argument
 * carefully --- we just slop it up in OT_WHOLE_LINE mode.)
 *
 * Returns -1 if no line number is present, 0 on error, or a positive value
 * on success.
 */
static int strip_lineno_from_funcdesc(char* func)
{
    char* c = NULL;
    int lineno;

    if (NULL == func || func[0] == '\0')
        return -1;

    c = func + strlen(func) - 1;

    /*
     * This business of parsing backwards is dangerous as can be in a
     * multibyte environment: there is no reason to believe that we are
     * looking at the first byte of a character, nor are we necessarily
     * working in a "safe" encoding.  Fortunately the bitpatterns we are
     * looking for are unlikely to occur as non-first bytes, but beware of
     * trying to expand the set of cases that can be recognized.  We must
     * guard the <ctype.h> macros by using isascii() first, too.
     */

    /* skip trailing whitespace */
    while (c > func && isascii((unsigned char)*c) && isspace((unsigned char)*c))
        c--;

    /* must have a digit as last non-space char */
    if (c == func || !isascii((unsigned char)*c) || !isdigit((unsigned char)*c))
        return -1;

    /* find start of digit string */
    while (c > func && isascii((unsigned char)*c) && isdigit((unsigned char)*c))
        c--;

    /* digits must be separated from func name by space or closing paren */
    /* notice also that we are not allowing an empty func name ... */
    if (c == func || !isascii((unsigned char)*c) || !(isspace((unsigned char)*c) || *c == ')'))
        return -1;

    /* parse digit string */
    c++;
    lineno = atoi(c);
    if (lineno < 1) {
        psql_error("invalid line number: %s\n", c);
        return 0;
    }

    /* strip digit string from func */
    *c = '\0';

    return lineno;
}

/*
 * Report just the primary error; this is to avoid cluttering the output
 * with, for instance, a redisplay of the internally generated query
 */
static void minimal_error_message(PGresult* res)
{
    PQExpBuffer msg = NULL;
    const char* fld = NULL;

    msg = createPQExpBuffer();

    fld = PQresultErrorField(res, PG_DIAG_SEVERITY);
    if (NULL != fld)
        printfPQExpBuffer(msg, "%s:  ", fld);
    else
        printfPQExpBuffer(msg, "ERROR:  ");
    fld = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    if (NULL != fld)
        appendPQExpBufferStr(msg, fld);
    else
        appendPQExpBufferStr(msg, "(not available)");
    appendPQExpBufferStr(msg, "\n");

    psql_error("%s", msg->data);

    destroyPQExpBuffer(msg);
}

#ifndef ENABLE_LITE_MODE
/* Show notice message when the password expired time will come. */
static void show_password_notify(PGconn* conn)
{
    static const char* stmt1 = "SELECT intervaltonum(gs_password_deadline())";
    static const char* stmt2 = "SELECT gs_password_notifytime()";
    static const char* date1 = NULL;
    static const char* date2 = NULL;
    double day1 = 0;
    int day2 = 0;
    int leftday = 0;
    PGresult* res1 = NULL;
    PGresult* res2 = NULL;
    bool checkornot = true;
    ExecStatusType resStatus;

    /* Get the password expired time from server. */
    res1 = PQexec(conn, stmt1);
    resStatus = PQresultStatus(res1);
    if (resStatus != PGRES_TUPLES_OK) {
        fprintf(stderr, "execute get deadline failed. resStatus=%d \n", resStatus);
        /* The error message above is not enough to locate what happened. */
        if (PQerrorMessage(conn) != NULL)
            fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
        PQclear(res1);
        PQfinish(conn);
        exit(1);
    }

    /* Get the password notice time from server. */
    res2 = PQexec(conn, stmt2);
    resStatus = PQresultStatus(res2);
    if (resStatus != PGRES_TUPLES_OK) {
        fprintf(stderr, "execute get notice time failed. resStatus=%d \n", resStatus);
        /* The error message above is not enough to locate what happened. */
        if (PQerrorMessage(conn) != NULL)
            fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
        PQclear(res2);
        PQfinish(conn);
        exit(1);
    }

    date1 = pg_strdup(PQgetvalue(res1, 0, 0));
    date2 = pg_strdup(PQgetvalue(res2, 0, 0));

    /* Get effect day1, notice day2 and left day for calculate. */
    day1 = atof(date1);
    day2 = atoi(date2);
    leftday = (int)ceil(day1);

    /* If password_effect_time or password_notice_time is zero, we won't check. */
    if (day1 == 0 || day2 == 0) {
        checkornot = false;
    }
    
    if (checkornot && day1 < 0) {
        /* The password has been expired, notice and do nothing. */
        printf("NOTICE : The password has been expired, please change the password. \n");
    } else if (checkornot && leftday <= day2) {
        /* The password expired time is coming in notice time range and we will notice user. */
        printf("NOTICE : %d days left before password expired, please change the password. \n", leftday);
    }

    PQclear(res1);
    PQclear(res2);
    free((void *)date1);
    free((void *)date2);
    return;
}
#endif

extern void client_server_version_check(PGconn* conn)
{
    static const char* stmt = "SELECT VERSION()";
    static const char* sversion = NULL;
    PGresult* res = NULL;
    ExecStatusType resStatus;

    /* Get the version of the server. */
    res = PQexec(conn, stmt);
    resStatus = PQresultStatus(res);
    if (resStatus != PGRES_TUPLES_OK) {
        fprintf(stderr, "Failed to get the server's version. resStatus=%d \n", resStatus);
        /* The error message above is not enough to locate what happened. */
        if (PQerrorMessage(conn) != NULL)
            fprintf(stderr, "ERROR: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    sversion = PQgetvalue(res, 0, 0);
    /* Find "(GaussDB Kernel VXXXRXXXCXX build XXXX)" first, and ignore "Postgres X.X.X". */
    sversion = strstr(sversion, DEF_GS_VERSION);
    /* Compare the versions between gsql-clinet and server */
    if (NULL != sversion && strncmp(sversion, DEF_GS_VERSION, 37) != 0) {
        printf(_("Warning: The client and server have different version numbers.\n"));
    }

    PQclear(res);
    return;
}

#ifdef ENABLE_UT
void uttest_show_password_notify(PGconn* conn)
{
    show_password_notify(conn);
}
#endif

/*
 * @Description: clear memory of sensitive data.
 * @in strings : the strings need clear.
 * @in freeflag : the flag to identify whether need free the memory.
 * @return : non-return.
 */
static void clear_sensitive_memory(char* strings, bool freeflag)
{
    if (strings != NULL) {
        errno_t rc = EOK;
        rc = memset_s(strings, strlen(strings), 0, strlen(strings));
        check_memset_s(rc);
        if (freeflag) {
            free(strings);
            strings = NULL;
        }
    }
    return;
}

#ifdef ENABLE_UT
void ut_clear_sensitive_mem()
{
    errno_t rc = EOK;
    char* p = (char*)malloc(10);
    if (NULL != p) {
        rc = strcpy_s(p, 10, "123456789");
        check_strcpy_s(rc);
        clear_sensitive_memory(p, true);
    }
}

#endif

