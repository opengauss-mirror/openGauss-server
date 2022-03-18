/* -------------------------------------------------------------------------
 *
 * hba.cpp
 *	  Routines to handle host based authentication (that's the scheme
 *	  wherein you authenticate a user by seeing what IP address the system
 *	  says he comes from and choosing authentication method based on it).
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/libpq/hba.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <pwd.h>
#include <fcntl.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "catalog/pg_collation.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/auth.h"
#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"
#include "regex/regex.h"
#include "replication/walsender.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "commands/user.h"
#include "libpq/crypt.h"


#define atooid(x) ((Oid)strtoul((x), NULL, 10))
#define atoxid(x) ((TransactionId)strtoul((x), NULL, 10))
#define token_is_keyword(t, k) (!(t)->quoted && strcmp((t)->string, (k)) == 0)
#define token_matches(t, k) (strcmp((t)->string, (k)) == 0)

#define MAX_TOKEN 256
#define MAX_HOST_NAME_LENGTH 255

/* callback data for check_network_callback */
typedef struct check_network_data {
    IPCompareMethod method; /* test method */
    SockAddr* raddr;        /* client's actual address */
    bool result;            /* set to true if match */
} check_network_data;

/*
 * A single string token lexed from the HBA config file, together with whether
 * the token had been quoted.
 */
typedef struct HbaToken {
    char* string;
    bool quoted;
} HbaToken;

static MemoryContext tokenize_file(const char* filename, FILE* file, List** lines, List** line_nums);
static List* tokenize_inc_file(List* tokens, const char* outer_filename, const char* inc_filename);
static bool parse_hba_auth_opt(char* name, char* val, HbaLine* hbaline);

extern bool ConnAuthMethodCorrect;

/*
 * isblank() exists in the ISO C99 spec, but it's not very portable yet,
 * so provide our own version.
 */
bool pg_isblank(const char c)
{
    return c == ' ' || c == '\t' || c == '\r';
}

/*
 * Grab one token out of fp. Tokens are strings of non-blank
 * characters bounded by blank characters, commas, beginning of line, and
 * end of line. Blank means space or tab. Tokens can be delimited by
 * double quotes (this allows the inclusion of blanks, but not newlines).
 *
 * The token, if any, is returned at *buf (a buffer of size bufsz).
 * Also, we set *initial_quote to indicate whether there was quoting before
 * the first character.  (We use that to prevent "@x" from being treated
 * as a file inclusion request.  Note that @"x" should be so treated;
 * we want to allow that to support embedded spaces in file paths.)
 * We set *terminating_comma to indicate whether the token is terminated by a
 * comma (which is not returned.)
 *
 * If successful: store null-terminated token at *buf and return TRUE.
 * If no more tokens on line: set *buf = '\0' and return FALSE.
 *
 * Leave file positioned at the character immediately after the token or EOF,
 * whichever comes first. If no more tokens on line, position the file to the
 * beginning of the next line or EOF, whichever comes first.
 *
 * Handle comments.
 */
static bool next_token(FILE* fp, char* buf, int bufsz, bool* initial_quote, bool* terminating_comma)
{
    int c;
    char* start_buf = buf;
    char* end_buf = buf + (bufsz - 2);
    bool in_quote = false;
    bool was_quote = false;
    bool saw_quote = false;

    /* end_buf reserves two bytes to ensure we can append \n and \0 */
    Assert(end_buf > start_buf);

    *initial_quote = false;
    *terminating_comma = false;

    /* Move over initial whitespace and commas */
    while ((c = getc(fp)) != EOF && (pg_isblank(c) || c == ','))
        ;

    if (c == EOF || c == '\n') {
        *buf = '\0';
        return false;
    }

    /*
     * Build a token in buf of next characters up to EOF, EOL, unquoted comma,
     * or unquoted whitespace.
     */
    while (c != EOF && c != '\n' && (!pg_isblank(c) || in_quote)) {
        /* skip comments to EOL */
        if (c == '#' && !in_quote) {
            while ((c = getc(fp)) != EOF && c != '\n')
                ;
            /* If only comment, consume EOL too; return EOL */
            if (c != EOF && buf == start_buf)
                c = getc(fp);
            break;
        }

        if (buf >= end_buf) {
            *buf = '\0';
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("authentication file token too long, skipping: \"%s\"", start_buf)));
            /* Discard remainder of line */
            while ((c = getc(fp)) != EOF && c != '\n')
                ;
            break;
        }

        /* we do not pass back the comma in the token */
        if (c == ',' && !in_quote) {
            *terminating_comma = true;
            break;
        }

        if (c != '"' || was_quote)
            *buf++ = c;

        /* Literal double-quote is two double-quotes */
        if (in_quote && c == '"')
            was_quote = !was_quote;
        else
            was_quote = false;

        if (c == '"') {
            in_quote = !in_quote;
            saw_quote = true;
            if (buf == start_buf)
                *initial_quote = true;
        }

        c = getc(fp);
    }

    /*
     * Put back the char right after the token (critical in case it is EOL,
     * since we need to detect end-of-line at next call).
     */
    if (c != EOF)
        ungetc(c, fp);

    *buf = '\0';

    return (saw_quote || buf > start_buf);
}

static HbaToken* make_hba_token(const char* token, bool quoted)
{
    HbaToken* hbatoken = NULL;
    int toklen;

    toklen = strlen(token);
    hbatoken = (HbaToken*)palloc(sizeof(HbaToken) + toklen + 1);
    hbatoken->string = (char*)hbatoken + sizeof(HbaToken);
    hbatoken->quoted = quoted;
    int rc = memcpy_s(hbatoken->string, toklen + 1, token, toklen + 1);
    securec_check(rc, "\0", "\0");

    return hbatoken;
}

/*
 * Copy a HbaToken struct into freshly palloc'd memory.
 */
static HbaToken* copy_hba_token(HbaToken* in)
{
    HbaToken* out = make_hba_token(in->string, in->quoted);

    return out;
}

/*
 * Tokenize one HBA field from a file, handling file inclusion and comma lists.
 *
 * The result is a List of HbaToken structs for each individual token,
 * or NIL if we reached EOL.
 */
static List* next_field_expand(const char* filename, FILE* file)
{
    char buf[MAX_TOKEN] = {0};
    bool trailing_comma = false;
    bool initial_quote = false;
    List* tokens = NIL;

    do {
        if (!next_token(file, buf, sizeof(buf), &initial_quote, &trailing_comma)) {
            break;
        }

        /* Is this referencing a file? */
        if (!initial_quote && buf[0] == '@' && buf[1] != '\0')
            tokens = tokenize_inc_file(tokens, filename, buf + 1);
        else
            tokens = lappend(tokens, make_hba_token(buf, initial_quote));
    } while (trailing_comma);

    return tokens;
}

/*
 * tokenize_inc_file
 *		Expand a file included from another file into an hba "field"
 *
 * Opens and tokenises a file included from another HBA config file with @,
 * and returns all values found therein as a flat list of HbaTokens.  If a
 * @-token is found, recursively expand it.  The given token list is used as
 * initial contents of list (so foo,bar,@baz does what you expect).
 */
static List* tokenize_inc_file(List* tokens, const char* outer_filename, const char* inc_filename)
{
    char* inc_fullname = NULL;
    FILE* inc_file = NULL;
    List* inc_lines = NIL;
    List* inc_line_nums = NIL;
    ListCell* inc_line = NULL;
    MemoryContext linecxt;
    int rc = 0;

    if (is_absolute_path(inc_filename)) {
        /* absolute path is taken as-is */
        inc_fullname = pstrdup(inc_filename);
    } else {
        /* relative path is relative to dir of calling file */
        inc_fullname = (char*)palloc(strlen(outer_filename) + 1 + strlen(inc_filename) + 1);
        rc = strcpy_s(inc_fullname, strlen(outer_filename) + 2 + strlen(inc_filename), outer_filename);
        securec_check(rc, "\0", "\0");

        get_parent_directory(inc_fullname);
        join_path_components(inc_fullname, inc_fullname, inc_filename);
        canonicalize_path(inc_fullname);
    }

    inc_file = AllocateFile(inc_fullname, "r");
    if (inc_file == NULL) {
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg(
                    "could not open secondary authentication file \"@%s\" as \"%s\": %m", inc_filename, inc_fullname)));
        pfree_ext(inc_fullname);
        return tokens;
    }

    /* There is possible recursion here if the file contains @ */
    linecxt = tokenize_file(inc_fullname, inc_file, &inc_lines, &inc_line_nums);

    (void)FreeFile(inc_file);
    pfree_ext(inc_fullname);

    foreach (inc_line, inc_lines) {
        List* inc_fields = (List*)lfirst(inc_line);
        ListCell* inc_field = NULL;

        foreach (inc_field, inc_fields) {
            List* inc_tokens = (List*)lfirst(inc_field);
            ListCell* inc_token = NULL;

            foreach (inc_token, inc_tokens) {
                HbaToken* token = (HbaToken*)lfirst(inc_token);

                tokens = lappend(tokens, copy_hba_token(token));
            }
        }
    }

    MemoryContextDelete(linecxt);
    return tokens;
}

/*
 * Tokenize the given file, storing the resulting data into two Lists: a
 * List of lines, and a List of line numbers.
 *
 * The list of lines is a triple-nested List structure.  Each line is a List of
 * fields, and each field is a List of HbaTokens.
 *
 * filename must be the absolute path to the target file.
 *
 * Return value is a memory context which contains all memory allocated by
 * this function.
 */
static MemoryContext tokenize_file(const char* filename, FILE* file, List** lines, List** line_nums)
{
    List* current_line = NIL;
    List* current_field = NIL;
    int line_number = 1;
    MemoryContext linecxt;
    MemoryContext oldcxt;

    linecxt = AllocSetContextCreate(u_sess->top_mem_cxt,
        "tokenize file cxt",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    oldcxt = MemoryContextSwitchTo(linecxt);

    *lines = *line_nums = NIL;

    while (!feof(file) && !ferror(file)) {
        current_field = next_field_expand(filename, file);
        /* add tokens to list, unless we are at EOL or comment start */
        if (list_length(current_field) > 0) {
            if (current_line == NIL) {
                /* make a new line List, record its line number */
                current_line = lappend(current_line, current_field);
                *lines = lappend(*lines, current_line);
                *line_nums = lappend_int(*line_nums, line_number);
            } else {
                /* append tokens to current line's list */
                current_line = lappend(current_line, current_field);
            }
        } else {
            /* we are at real or logical EOL, so force a new line List */
            current_line = NIL;
            line_number++;
        }
    }

    MemoryContextSwitchTo(oldcxt);

    return linecxt;
}

/*
 * Does user belong to role?
 *
 * userid is the OID of the role given as the attempted login identifier.
 * We check to see if it is a member of the specified role name.
 */
static bool is_member(Oid userid, const char* role)
{
    Oid roleid;

    if (!OidIsValid(userid))
        return false; /* if user not exist, say "no" */

    roleid = get_role_oid(role, true);

    if (!OidIsValid(roleid))
        return false; /* if target role not exist, say "no" */

    /*
     * See if user is directly or indirectly a member of role. For this
     * purpose, a superuser is not considered to be automatically a member of
     * the role, so group auth only applies to explicit membership.
     */
    return is_member_of_role_nosuper(userid, roleid);
}

/*
 * Check HbaToken list for a match to role, allowing group names.
 */
static bool check_role(const char* role, Oid roleid, List* tokens)
{
    ListCell* cell = NULL;
    HbaToken* tok = NULL;

    foreach (cell, tokens) {
        tok = (HbaToken*)lfirst(cell);
        if (!tok->quoted && tok->string[0] == '+') {
            if (is_member(roleid, tok->string + 1))
                return true;
        } else if (token_matches(tok, role) || token_is_keyword(tok, "all"))
            return true;
    }
    return false;
}

/*
 * Check to see if db/role combination matches HbaToken list.
 */
static bool check_db(const char* dbname, const char* role, Oid roleid, List* tokens)
{
    ListCell* cell = NULL;
    HbaToken* tok = NULL;

    foreach (cell, tokens) {
        tok = (HbaToken*)lfirst(cell);
        if (AM_WAL_SENDER) {
            /* walsender connections can only match replication keyword */
            if (token_is_keyword(tok, "replication"))
                return true;
        } else if (token_is_keyword(tok, "all"))
            return true;
        else if (token_is_keyword(tok, "sameuser")) {
            if (strcmp(dbname, role) == 0)
                return true;
        } else if (token_is_keyword(tok, "samegroup") || token_is_keyword(tok, "samerole")) {
            if (is_member(roleid, dbname))
                return true;
        } else if (token_is_keyword(tok, "replication"))
            continue; /* never match this if not walsender */
        else if (token_matches(tok, dbname))
            return true;
    }
    return false;
}

static bool ipv4eq(struct sockaddr_in* a, struct sockaddr_in* b)
{
    return (a->sin_addr.s_addr == b->sin_addr.s_addr);
}

#ifdef HAVE_IPV6

static bool ipv6eq(struct sockaddr_in6* a, struct sockaddr_in6* b)
{
    int i;

    for (i = 0; i < 16; i++)
        if (a->sin6_addr.s6_addr[i] != b->sin6_addr.s6_addr[i])
            return false;

    return true;
}
#endif /* HAVE_IPV6 */

/*
 * Check whether host name matches pattern.
 */
static bool hostname_match(const char* pattern, const char* actual_hostname)
{
	/* suffix match */
    if (pattern[0] == '.') {
        size_t plen = strlen(pattern);
        size_t hlen = strlen(actual_hostname);
        if (hlen < plen) {
            return false;
        }

        return (pg_strcasecmp(pattern, actual_hostname + (hlen - plen)) == 0);
    } else {
        return (pg_strcasecmp(pattern, actual_hostname) == 0);
    }
}

/*
 * Check to see if a connecting IP matches a given host name.
 */
static bool check_hostname(hbaPort* port, const char* hostname)
{
    struct addrinfo *gai_result = NULL, *gai = NULL;
    int ret = -1;
    bool found = false;

    /* Lookup remote host name if not already done */
    if (port->remote_hostname == NULL) {
        char remote_hostname[NI_MAXHOST] = {0};

        if (pg_getnameinfo_all(
                &port->raddr.addr, port->raddr.salen, remote_hostname, sizeof(remote_hostname), NULL, 0, 0) != 0)
            return false;

        port->remote_hostname = pstrdup(remote_hostname);
    }

    if (!hostname_match(hostname, port->remote_hostname))
        return false;

    /* Lookup IP from host name and check against original IP */
    if (port->remote_hostname_resolv == +1)
        return true;
    if (port->remote_hostname_resolv == -1)
        return false;

    ret = getaddrinfo(port->remote_hostname, NULL, NULL, &gai_result);
    if (ret != 0) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg(
                    "could not translate host name \"%s\" to address: %s", port->remote_hostname, gai_strerror(ret))));
        return false;
    }

    for (gai = gai_result; gai; gai = gai->ai_next) {
        if (gai->ai_addr->sa_family == port->raddr.addr.ss_family) {
            if (gai->ai_addr->sa_family == AF_INET) {
                if (ipv4eq((struct sockaddr_in*)gai->ai_addr, (struct sockaddr_in*)&port->raddr.addr)) {
                    found = true;
                    break;
                }
            }
#ifdef HAVE_IPV6
            else if (gai->ai_addr->sa_family == AF_INET6) {
                if (ipv6eq((struct sockaddr_in6*)gai->ai_addr, (struct sockaddr_in6*)&port->raddr.addr)) {
                    found = true;
                    break;
                }
            }
#endif
        }
    }

    if (gai_result != NULL)
        freeaddrinfo(gai_result);

    if (!found)
        elog(DEBUG2,
            "pg_hba.conf host name \"%s\" rejected because address resolution did not return a match with IP address "
            "of client",
            hostname);

    port->remote_hostname_resolv = found ? +1 : -1;

    return found;
}

/*
 * Check to see if a connecting IP matches the given address and netmask.
 */
static bool check_ip(SockAddr* raddr, struct sockaddr* addr, struct sockaddr* mask)
{
    errno_t errorno = EOK;
    if (raddr->addr.ss_family == addr->sa_family) {
        /* Same address family */
        if (!pg_range_sockaddr(&raddr->addr, (struct sockaddr_storage*)addr, (struct sockaddr_storage*)mask))
            return false;
    }
#ifdef HAVE_IPV6
    else if (addr->sa_family == AF_INET && raddr->addr.ss_family == AF_INET6) {
        /*
         * If we're connected on IPv6 but the file specifies an IPv4 address
         * to match against, promote the latter to an IPv6 address before
         * trying to match the client's address.
         */
        struct sockaddr_storage addrcopy, maskcopy;

        errorno = memcpy_s(&addrcopy, sizeof(sockaddr_storage), addr, sizeof(addrcopy));
        securec_check(errorno, "\0", "\0");
        errorno = memcpy_s(&maskcopy, sizeof(sockaddr_storage), mask, sizeof(maskcopy));
        securec_check(errorno, "\0", "\0");

        pg_promote_v4_to_v6_addr(&addrcopy);
        pg_promote_v4_to_v6_mask(&maskcopy);

        if (!pg_range_sockaddr(&raddr->addr, &addrcopy, &maskcopy))
            return false;
    }
#endif /* HAVE_IPV6 */
    else {
        /* Wrong address family, no IPV6 */
        return false;
    }

    return true;
}

/*
 * pg_foreach_ifaddr callback: does client addr match this machine interface?
 */
static void check_network_callback(struct sockaddr* addr, struct sockaddr* netmask, void* cb_data)
{
    check_network_data* cn = (check_network_data*)cb_data;
    struct sockaddr_storage mask;

    /* Already found a match? */
    if (cn->result) {
        return;
    }

    if (cn->method == ipCmpSameHost) {
        /* Make an all-ones netmask of appropriate length for family */
        pg_sockaddr_cidr_mask(&mask, NULL, addr->sa_family);
        cn->result = check_ip(cn->raddr, addr, (struct sockaddr*)&mask);
    } else {
        /* Use the netmask of the interface itself */
        cn->result = check_ip(cn->raddr, addr, netmask);
    }
}

/*
 * Use pg_foreach_ifaddr to check a samehost or samenet match
 */
static bool check_same_host_or_net(SockAddr* raddr, IPCompareMethod method)
{
    check_network_data cn;

    cn.method = method;
    cn.raddr = raddr;
    cn.result = false;

    errno = 0;
    if (pg_foreach_ifaddr(check_network_callback, &cn) < 0) {
        ereport(LOG, (errmsg("error enumerating network interfaces: %m")));
        return false;
    }

    return cn.result;
}

/*
 * Macros used to check and report on invalid configuration options.
 * INVALID_AUTH_OPTION = reports when an option is specified for a method where it's
 *						 not supported.
 * REQUIRE_AUTH_OPTION = same as INVALID_AUTH_OPTION, except it also checks if the
 *						 method is actually the one specified. Used as a shortcut when
 *						 the option is only valid for one authentication method.
 * MANDATORY_AUTH_ARG  = check if a required option is set for an authentication method,
 *						 reporting error if it's not.
 */
#define INVALID_AUTH_OPTION(optname, validmethods)                                                                \
    do {                                                                                                          \
        ereport(LOG,                                                                                              \
            (errcode(ERRCODE_CONFIG_FILE_ERROR), /* translator: the second %s is a list of auth methods */        \
                errmsg("authentication option \"%s\" is only valid for authentication methods %s",                \
                    optname,                                                                                      \
                    _(validmethods)),                                                                             \
                errcontext(                                                                                       \
                    "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName))); \
        return false;                                                                                             \
    } while (0);

#define REQUIRE_AUTH_OPTION(methodval, optname, validmethods) \
    do {                                                      \
        if (hbaline->auth_method != (methodval))              \
            INVALID_AUTH_OPTION((optname), (validmethods));   \
    } while (0);

#define MANDATORY_AUTH_ARG(argvar, argname, authname)                                                                 \
    do {                                                                                                              \
        if ((argvar) == NULL) {                                                                                       \
            ereport(LOG,                                                                                              \
                (errcode(ERRCODE_CONFIG_FILE_ERROR),                                                                  \
                    errmsg("authentication method \"%s\" requires argument \"%s\" to be set", (authname), (argname)), \
                    errcontext(                                                                                       \
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName))); \
            return NULL;                                                                                              \
        }                                                                                                             \
    } while (0);

/*
 * IDENT_FIELD_ABSENT:
 * Throw an error and exit the function if the given ident field ListCell is
 * not populated.
 *
 * IDENT_MULTI_VALUE:
 * Throw an error and exit the function if the given ident token List has more
 * than one element.
 */
#define IDENT_FIELD_ABSENT(field)                                            \
    do {                                                                     \
        if ((field) == NULL) {                                               \
            ereport(LOG,                                                     \
                (errcode(ERRCODE_CONFIG_FILE_ERROR),                         \
                    errmsg("missing entry in file \"%s\" at end of line %d", \
                        g_instance.attr.attr_common.IdentFileName,           \
                        line_number)));                                      \
            *error_p = true;                                                 \
            return;                                                          \
        }                                                                    \
    } while (0);

#define IDENT_MULTI_VALUE(tokens)                                      \
    do {                                                               \
        if ((tokens)->length > 1) {                                    \
            ereport(LOG,                                               \
                (errcode(ERRCODE_CONFIG_FILE_ERROR),                   \
                    errmsg("multiple values in ident field"),          \
                    errcontext("line %d of configuration file \"%s\"", \
                        line_number,                                   \
                        g_instance.attr.attr_common.IdentFileName)));  \
            *error_p = true;                                           \
            return;                                                    \
        }                                                              \
    } while (0);

static void hba_rwlock_cleanup(int code, Datum arg)
{
    (void)pthread_rwlock_unlock(&hba_rwlock);
}

/*
 * Parse one tokenised line from the hba config file and store the result in a
 * HbaLine structure, or NULL if parsing fails.
 *
 * The tokenised line is a List of fields, each field being a List of
 * HbaTokens.
 *
 * Note: this function leaks memory when an error occurs.  Caller is expected
 * to have set a memory context that will be reset if this function returns
 * NULL.
 */
static HbaLine* parse_hba_line(List* line, int line_num)
{
#define MAXLISTEN 64
    char* str = NULL;
    struct addrinfo* gai_result = NULL;
    struct addrinfo hints;
    int ret;
    errno_t rc = 0;
    char* cidr_slash = NULL;
    char* unsupauth = NULL;
    ListCell* field = NULL;
    List* tokens = NIL;
    ListCell* tokencell = NULL;
    HbaToken* token = NULL;
    HbaLine* parsedline = NULL;
    char tmp_str[MAXLISTEN] = {0};
    parsedline = (HbaLine*)palloc0(sizeof(HbaLine));
    parsedline->linenumber = line_num;

    /* Check the record type. */
    field = list_head(line);
    tokens = (List*)lfirst(field);
    if (tokens->length > 1) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("multiple values specified for connection type"),
                errhint("Specify exactly one connection type per line."),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }
    token = (HbaToken*)linitial(tokens);
    if (strcmp(token->string, "local") == 0) {
#ifdef HAVE_UNIX_SOCKETS
        parsedline->conntype = ctLocal;
#else
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("local connections are not supported by this build"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
#endif
    } else if (strcmp(token->string, "host") == 0 || strcmp(token->string, "hostssl") == 0 ||
               strcmp(token->string, "hostnossl") == 0) {

        if (token->string[4] == 's') /* "hostssl" */
        {
            /* SSL support must be actually active, else complain */
#ifdef USE_SSL
            parsedline->conntype = ctHostSSL;
            if (!g_instance.attr.attr_security.EnableSSL) {
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("hostssl requires SSL to be turned on"),
                        errhint("Set ssl = on in postgresql.conf."),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
            }
#else
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("hostssl is not supported by this build"),
                    errhint("Compile with --with-openssl to use SSL connections."),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return NULL;
#endif
        }
#ifdef USE_SSL
        else if (token->string[4] == 'n') /* "hostnossl" */
        {
            parsedline->conntype = ctHostNoSSL;
        }
#endif
        else {
            /* "host", or "hostnossl" and SSL support not built in */
            parsedline->conntype = ctHost;
        }
    } /* record type */
    else {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("invalid connection type \"%s\"", token->string),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    /* Get the databases. */
    field = lnext(field);
    if (field == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("end-of-line before database specification"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }
    parsedline->databases = NIL;
    tokens = (List*)lfirst(field);
    foreach (tokencell, tokens) {
        parsedline->databases = lappend(parsedline->databases, copy_hba_token((HbaToken*)lfirst(tokencell)));
    }

    /* Get the roles. */
    field = lnext(field);
    if (field == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("end-of-line before role specification"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }
    parsedline->roles = NIL;
    tokens = (List*)lfirst(field);
    foreach (tokencell, tokens) {
        parsedline->roles = lappend(parsedline->roles, copy_hba_token((HbaToken*)lfirst(tokencell)));
    }

    if (parsedline->conntype != ctLocal) {
        /* Read the IP address field. (with or without CIDR netmask) */
        field = lnext(field);
        if (field == NULL) {
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("end-of-line before IP address specification"),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return NULL;
        }
        tokens = (List*)lfirst(field);
        if (tokens->length > 1) {
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("multiple values specified for host address"),
                    errhint("Specify one address range per line."),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return NULL;
        }
        token = (HbaToken*)linitial(tokens);

        if (token_is_keyword(token, "all")) {
            parsedline->ip_cmp_method = ipCmpAll;
        } else if (token_is_keyword(token, "samehost")) {
            /* Any IP on this host is allowed to connect */
            parsedline->ip_cmp_method = ipCmpSameHost;
        } else if (token_is_keyword(token, "samenet")) {
            /* Any IP on the host's subnets is allowed to connect */
            parsedline->ip_cmp_method = ipCmpSameNet;
        } else {
            /* IP and netmask are specified */
            parsedline->ip_cmp_method = ipCmpMask;

            /* need a modifiable copy of token */
            str = pstrdup(token->string);

            /* Check if it has a CIDR suffix and if so isolate it */
            cidr_slash = strchr(str, '/');
            /* Database Security:  Disable remote connection with trust method. */
            if (cidr_slash != NULL)
                *cidr_slash = '\0';
            rc = strncpy_s(tmp_str, MAXLISTEN, str, MAXLISTEN - 1);
            securec_check(rc, "\0", "\0");
            tmp_str[MAXLISTEN - 1] = '\0';

            /* Get the IP address either way */
            hints.ai_flags = AI_NUMERICHOST;
            hints.ai_family = PF_UNSPEC;
            hints.ai_socktype = 0;
            hints.ai_protocol = 0;
            hints.ai_addrlen = 0;
            hints.ai_canonname = NULL;
            hints.ai_addr = NULL;
            hints.ai_next = NULL;

            ret = pg_getaddrinfo_all(str, NULL, &hints, &gai_result);
            if (ret == 0 && gai_result) {
                rc = memcpy_s(&parsedline->addr, gai_result->ai_addrlen, gai_result->ai_addr, gai_result->ai_addrlen);
                securec_check(rc, "\0", "\0");
            } else if (ret == EAI_NONAME)
                parsedline->hostname = str;
            else {
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("invalid IP address \"%s\": %s", str, gai_strerror(ret)),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
                if (gai_result != NULL)
                    pg_freeaddrinfo_all(hints.ai_family, gai_result);
                return NULL;
            }

            pg_freeaddrinfo_all(hints.ai_family, gai_result);

            /* Get the netmask */
            if (cidr_slash != NULL) {
                if (parsedline->hostname != NULL) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("specifying both host name and CIDR mask is invalid: \"%s\"", token->string),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    return NULL;
                }

                if (pg_sockaddr_cidr_mask(&parsedline->mask, cidr_slash + 1, parsedline->addr.ss_family) < 0) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("invalid CIDR mask in address \"%s\"", token->string),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    return NULL;
                }
                pfree_ext(str);
            } else if (!parsedline->hostname) {
                /* Read the mask field. */
                pfree_ext(str);
                field = lnext(field);
                if (field == NULL) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("end-of-line before netmask specification"),
                            errhint("Specify an address range in CIDR notation, or provide a separate netmask."),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    return NULL;
                }
                tokens = (List*)lfirst(field);
                if (tokens->length > 1) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("multiple values specified for netmask"),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    return NULL;
                }
                token = (HbaToken*)linitial(tokens);

                ret = pg_getaddrinfo_all(token->string, NULL, &hints, &gai_result);
                if (ret || gai_result == NULL) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("invalid IP mask \"%s\": %s", token->string, gai_strerror(ret)),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    if (gai_result != NULL)
                        pg_freeaddrinfo_all(hints.ai_family, gai_result);
                    return NULL;
                }

                rc = memcpy_s(&parsedline->mask, sizeof(parsedline->mask), gai_result->ai_addr, gai_result->ai_addrlen);
                securec_check(rc, "\0", "\0");
                pg_freeaddrinfo_all(hints.ai_family, gai_result);

                if (parsedline->addr.ss_family != parsedline->mask.ss_family) {
                    ereport(LOG,
                        (errcode(ERRCODE_CONFIG_FILE_ERROR),
                            errmsg("IP address and mask do not match"),
                            errcontext("line %d of configuration file \"%s\"",
                                line_num,
                                g_instance.attr.attr_common.HbaFileName)));
                    return NULL;
                }
            }
        }
    } /* != ctLocal */

    /* Get the authentication method */
    field = lnext(field);
    if (field == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("end-of-line before authentication method"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }
    tokens = (List*)lfirst(field);
    if (tokens->length > 1) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("multiple values specified for authentication type"),
                errhint("Specify exactly one authentication type per line."),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }
    token = (HbaToken*)linitial(tokens);

    unsupauth = NULL;
    /* Database Security:  Disable remote connection with trust method. */
    if (strcmp(token->string, "trust") == 0) {
        parsedline->auth_method = uaTrust;
        if (!(ctLocal == parsedline->conntype || 0 == strcmp(tmp_str, LOOP_IP_STRING) ||
                0 == strcmp(tmp_str, LOOP_IPV6_IP))) {
            parsedline->remoteTrust = true;
        }
    } else if (strcmp(token->string, "ident") == 0)
#ifdef USE_IDENT
        parsedline->auth_method = uaIdent;
#else
        unsupauth = "ident";
#endif
    else if (strcmp(token->string, "peer") == 0)
        parsedline->auth_method = uaPeer;
    else if (strcmp(token->string, "krb5") == 0)
#ifdef KRB5
        parsedline->auth_method = uaKrb5;
#else
        unsupauth = "krb5";
#endif
    else if (strcmp(token->string, "gss") == 0)
#ifdef ENABLE_GSS
        parsedline->auth_method = uaGSS;
#else
        unsupauth = "gss";
#endif
    else if (strcmp(token->string, "sspi") == 0)
#ifdef ENABLE_SSPI
        parsedline->auth_method = uaSSPI;
#else
        unsupauth = "sspi";
#endif
    else if (strcmp(token->string, "reject") == 0)
        parsedline->auth_method = uaReject;
    else if (strcmp(token->string, "md5") == 0)
        parsedline->auth_method = uaMD5;
    else if (strcmp(token->string, "sha256") == 0) {
        if (parsedline->conntype == ctLocal && u_sess->proc_cxt.IsInnerMaintenanceTools)
            parsedline->auth_method = uaTrust;
        else
            parsedline->auth_method = uaSHA256;
    } else if (strcmp(token->string, "sm3") == 0) {
        if (parsedline->conntype == ctLocal && u_sess->proc_cxt.IsInnerMaintenanceTools)
            parsedline->auth_method = uaTrust;
        else
            parsedline->auth_method = uaSM3;
    } else if (strcmp(token->string, "pam") == 0)
#ifdef USE_PAM
        parsedline->auth_method = uaPAM;
#else
        unsupauth = "pam";
#endif
    else if (strcmp(token->string, "ldap") == 0)
#ifdef USE_LDAP
        parsedline->auth_method = uaLDAP;
#else
        unsupauth = "ldap";
#endif
    else if (strcmp(token->string, "cert") == 0)
#ifdef USE_SSL
        parsedline->auth_method = uaCert;
#else
        unsupauth = "cert";
#endif
    else {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("invalid authentication method \"%s\"", token->string),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    if (unsupauth != NULL) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("invalid authentication method \"%s\": not supported by this build", token->string),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    /*
     * XXX: When using ident on local connections, change it to peer, for
     * backwards compatibility.
     */
#ifdef USE_IDENT

    if (parsedline->conntype == ctLocal && parsedline->auth_method == uaIdent)
        parsedline->auth_method = uaPeer;
#endif
    /* Invalid authentication combinations */
    if (parsedline->conntype == ctLocal && parsedline->auth_method == uaKrb5) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("krb5 authentication is not supported on local sockets"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    if (parsedline->conntype == ctLocal && parsedline->auth_method == uaGSS) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("gssapi authentication is not supported on local sockets"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    if (parsedline->conntype != ctLocal && parsedline->auth_method == uaPeer) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("peer authentication is only supported on local sockets"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    /*
     * SSPI authentication can never be enabled on ctLocal connections,
     * because it's only supported on Windows, where ctLocal isn't supported.
     */
    if (parsedline->conntype != ctHostSSL && parsedline->auth_method == uaCert) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("cert authentication is only supported on hostssl connections"),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return NULL;
    }

    /* Parse remaining arguments */
    while ((field = lnext(field)) != NULL) {
        tokens = (List*)lfirst(field);
        foreach (tokencell, tokens) {
            char* val = NULL;

            token = (HbaToken*)lfirst(tokencell);

            str = pstrdup(token->string);
            val = strchr(str, '=');
            if (val == NULL) {
                /*
                 * Got something that's not a name=value pair.
                 */
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("authentication option not in name=value format: %s", token->string),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
                return NULL;
            }

            *val++ = '\0'; /* str now holds "name", val holds "value" */
            if (!parse_hba_auth_opt(str, val, parsedline))
                /* parse_hba_auth_opt already logged the error message */
                return NULL;
            pfree_ext(str);
        }
    }

    /*
     * Check if the selected authentication method has any mandatory arguments
     * that are not set.
     */
    if (parsedline->auth_method == uaLDAP) {
        MANDATORY_AUTH_ARG(parsedline->ldapserver, "ldapserver", "ldap");

        /*
         * LDAP can operate in two modes: either with a direct bind, using
         * ldapprefix and ldapsuffix, or using a search+bind, using
         * ldapbasedn, ldapbinddn, ldapbindpasswd and ldapsearchattribute.
         * Disallow mixing these parameters.
         */
        if (parsedline->ldapprefix != NULL || parsedline->ldapsuffix != NULL) {
            if (parsedline->ldapbasedn != NULL || parsedline->ldapbinddn != NULL ||
                parsedline->ldapbindpasswd != NULL || parsedline->ldapsearchattribute != NULL) {
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("cannot use ldapbasedn, ldapbinddn, ldapbindpasswd, or ldapsearchattribute together "
                               "with ldapprefix"),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
                return NULL;
            }
        } else if (parsedline->ldapbasedn == NULL) {
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or "
                           "\"ldapsuffix\" to be set"),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return NULL;
        }
    }

    /*
     * Enforce any parameters implied by other settings.
     */
    if (parsedline->auth_method == uaCert) {
        parsedline->clientcert = true;
    }

    return parsedline;
}

/*
 * Parse one name-value pair as an authentication option into the given
 * HbaLine.  Return true if we successfully parse the option, false if we
 * encounter an error.
 */
static bool parse_hba_auth_opt(char* name, char* val, HbaLine* hbaline)
{
    int line_num = hbaline->linenumber;
    if (strcmp(name, "map") == 0) {
        if (hbaline->auth_method != uaIdent && hbaline->auth_method != uaPeer && hbaline->auth_method != uaKrb5 &&
            hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI && hbaline->auth_method != uaCert)
            INVALID_AUTH_OPTION("map", gettext_noop("ident, peer, krb5, gssapi, sspi, and cert"));
        hbaline->usermap = pstrdup(val);
    } else if (strcmp(name, "clientcert") == 0) {
        /*
         * Since we require ctHostSSL, this really can never happen on
         * non-SSL-enabled builds, so don't bother checking for USE_SSL.
         */
        if (hbaline->conntype != ctHostSSL) {
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("clientcert can only be configured for \"hostssl\" rows"),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return false;
        }
        if (strcmp(val, "1") == 0) {
            if (!secure_loaded_verify_locations()) {
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("client certificates can only be checked if a root certificate store is available"),
                        errhint("Make sure the configuration parameter \"ssl_ca_file\" is set."),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
                return false;
            }
            hbaline->clientcert = true;
        } else {
            if (hbaline->auth_method == uaCert) {
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("clientcert can not be set to 0 when using \"cert\" authentication"),
                        errcontext("line %d of configuration file \"%s\"",
                            line_num,
                            g_instance.attr.attr_common.HbaFileName)));
                return false;
            }
            hbaline->clientcert = false;
        }
    } else if (strcmp(name, "pamservice") == 0) {
        REQUIRE_AUTH_OPTION(uaPAM, "pamservice", "pam");
        hbaline->pamservice = pstrdup(val);
    } else if (strcmp(name, "ldaptls") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldaptls", "ldap");
        if (strcmp(val, "1") == 0)
            hbaline->ldaptls = true;
        else
            hbaline->ldaptls = false;
    } else if (strcmp(name, "ldapserver") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapserver", "ldap");
        hbaline->ldapserver = pstrdup(val);
    } else if (strcmp(name, "ldapport") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapport", "ldap");
        hbaline->ldapport = atoi(val);
        if (hbaline->ldapport == 0) {
            ereport(LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                    errmsg("invalid LDAP port number: \"%s\"", val),
                    errcontext(
                        "line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
            return false;
        }
    } else if (strcmp(name, "ldapbinddn") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbinddn", "ldap");
        hbaline->ldapbinddn = pstrdup(val);
    } else if (strcmp(name, "ldapbindpasswd") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbindpasswd", "ldap");
        hbaline->ldapbindpasswd = pstrdup(val);
    } else if (strcmp(name, "ldapsearchattribute") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapsearchattribute", "ldap");
        hbaline->ldapsearchattribute = pstrdup(val);
    } else if (strcmp(name, "ldapbasedn") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbasedn", "ldap");
        hbaline->ldapbasedn = pstrdup(val);
    } else if (strcmp(name, "ldapprefix") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapprefix", "ldap");
        hbaline->ldapprefix = pstrdup(val);
    } else if (strcmp(name, "ldapsuffix") == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapsuffix", "ldap");
        hbaline->ldapsuffix = pstrdup(val);
    } else if (strcmp(name, "krb_server_hostname") == 0) {
        REQUIRE_AUTH_OPTION(uaKrb5, "krb_server_hostname", "krb5");
        hbaline->krb_server_hostname = pstrdup(val);
    } else if (strcmp(name, "krb_realm") == 0) {
        if (hbaline->auth_method != uaKrb5 && hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI)
            INVALID_AUTH_OPTION("krb_realm", gettext_noop("krb5, gssapi, and sspi"));
        hbaline->krb_realm = pstrdup(val);
    } else if (strcmp(name, "include_realm") == 0) {
        if (hbaline->auth_method != uaKrb5 && hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI)
            INVALID_AUTH_OPTION("include_realm", gettext_noop("krb5, gssapi, and sspi"));
        if (strcmp(val, "1") == 0)
            hbaline->include_realm = true;
        else
            hbaline->include_realm = false;
    } else {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("unrecognized authentication option name: \"%s\"", name),
                errcontext("line %d of configuration file \"%s\"", line_num, g_instance.attr.attr_common.HbaFileName)));
        return false;
    }
    return true;
}

/* copy strings in HbaLine from "hba parser context"(instance level) to CurrentMemoryContext(session level) */
static void copy_hba_line(HbaLine* hba)
{
    hba->databases = NIL;
    hba->roles = NIL;
    hba->hostname = pstrdup(hba->hostname);
    hba->usermap = pstrdup(hba->usermap);
    hba->pamservice = pstrdup(hba->pamservice);
    hba->ldapserver = pstrdup(hba->ldapserver);
    hba->ldapbinddn = pstrdup(hba->ldapbinddn);
    hba->ldapbindpasswd = pstrdup(hba->ldapbindpasswd);
    hba->ldapsearchattribute = pstrdup(hba->ldapsearchattribute);
    hba->ldapbasedn = pstrdup(hba->ldapbasedn);
    hba->ldapprefix = pstrdup(hba->ldapprefix);
    hba->ldapsuffix = pstrdup(hba->ldapsuffix);
    hba->krb_server_hostname = pstrdup(hba->krb_server_hostname);
    hba->krb_realm = pstrdup(hba->krb_realm);
}

static void check_hba_replication(hbaPort* port)
{
    ListCell* cell = NULL;
    ListCell* line = NULL;
    HbaLine* hba = NULL;
    bool gotRepl = false;
    int ret;

    hba = (HbaLine*)palloc0(sizeof(HbaLine));
    /* we do not bother local connection with GSS authentification */
    if (IS_AF_UNIX(port->raddr.addr.ss_family)) {
        goto DIRECT_TRUST;
    }

    /* get replication method */
    ret = pthread_rwlock_rdlock(&hba_rwlock);
    if (ret != 0) {
        pfree_ext(hba);
        ereport(ERROR,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("get hba rwlock failed when check hba replication, return value:%d", ret)));
    }

    /* hba_rwlock will be released when ereport ERROR or FATAL. */
    PG_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    foreach (line, g_instance.libpq_cxt.comm_parsed_hba_lines) {
        /* 
         * memory copy here will not copy pointer types like List* and char*, 
         * the char* type in HbaLine will copy to session memctx by copy_hba_line()
         */
        errno_t rc = memcpy_s(hba, sizeof(HbaLine), lfirst(line), sizeof(HbaLine));
        securec_check(rc, "\0", "\0");

        foreach (cell, hba->databases) {
            HbaToken* tok = NULL;
            tok = (HbaToken*)lfirst(cell);
            if (token_is_keyword(tok, "replication") && hba->conntype != ctLocal) {
                if (hba->auth_method != uaGSS && !AM_WAL_HADR_SENDER && !AM_WAL_HADR_CN_SENDER) {
                    hba->auth_method = uaTrust;
                }
                ereport(LOG,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                        errmsg("walsender current auth_method is : %d", hba->auth_method)));
                gotRepl = true;
                break;
            }
        }
        if (gotRepl) {
            break;
        }
    }

    copy_hba_line(hba);
    PG_END_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    (void)pthread_rwlock_unlock(&hba_rwlock);

DIRECT_TRUST:
    /* cannot get invalid method, trust as default. */
    if (!gotRepl) {
        hba->auth_method = uaTrust;
    }
    port->hba = hba;
    return;
}

/* Max size of ipv4 ip address */
#define IPV4_MAXLEN 32
/* Max size of ipv6 ip address */
#define IPV6_MAXLEN 128

/*
 * check whether the login connection is from ip 127.0.0.1 or ::1.
 */
bool IsLoopBackAddr(Port* port)
{
    const SockAddr remote_addr = port->raddr;
    char remote_ip[IP_LEN] = {0};

    (void)pg_getnameinfo_all(&remote_addr.addr, remote_addr.salen, remote_ip, sizeof(remote_ip), NULL, 0,
                             NI_NUMERICHOST | NI_NUMERICSERV);

    /* only the remote ip equal to 127.0.0.1 or ::1, return true */
    if (strlen(remote_ip) != 0 && (strcmp(remote_ip, LOOP_IP_STRING) == 0 || strcmp(remote_ip, LOOP_IPV6_IP) == 0)) {
        return true;
    } else {
        return false;
    }
}

/* Mask the user be a NULL user if the uid is zero */
#define USER_NULL_MASK 0xFFFFFFFF
/* Max size of username operator system can return */
#define SYS_USERNAME_MAX 512

static bool IsSysUsernameSameToDB(uid_t uid, const char* dbUserName)
{
    errno_t rc = 0;
    char sysUser[SYS_USERNAME_MAX + 1] = {0};
    /* get the actual user name according to the user id */
    errno = 0;
    struct passwd pwtmp;
    struct passwd* pw = NULL;
    const int pwbufsz = sysconf(_SC_GETPW_R_SIZE_MAX);
    char* pwbuf = (char*)palloc0(pwbufsz);
    (void)getpwuid_r(uid, &pwtmp, pwbuf, pwbufsz, &pw);

    /* Here we discard pwret, as if any record in passwd found, pw is always a non-NULL value. */
    if (pw == NULL) {
        pfree_ext(pwbuf);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("could not look up local user ID %ld: %s",
                    (long)uid, errno ? gs_strerror(errno) : _("user does not exist"))));
    }

    /* record the system username */
    rc = strcpy_s(sysUser, SYS_USERNAME_MAX + 1, pw->pw_name);
    pfree_ext(pwbuf);
    securec_check(rc, "\0", "\0");
    if (strcmp(dbUserName, sysUser) == 0) {
        return true;
    } else {
        return false;
    }
}

static UserAuth get_default_auth_method(const char* role)
{
    int32 storedMethod;
    UserAuth defaultAuthMethod = uaSHA256;
    char encryptString[ENCRYPTED_STRING_LENGTH + 1] = {0};
    errno_t rc;

    storedMethod = get_password_stored_method(role, encryptString, ENCRYPTED_STRING_LENGTH + 1);
    switch (storedMethod) {
        case SHA256_PASSWORD:
            defaultAuthMethod = uaSHA256;
            break;
        case COMBINED_PASSWORD:
            defaultAuthMethod = uaSHA256;
            break;
        case SM3_PASSWORD:
            defaultAuthMethod = uaSM3;
            break;
        case MD5_PASSWORD:
            defaultAuthMethod = uaMD5;
            break;
        default:
            defaultAuthMethod = uaSHA256;
            break;
    }

    rc = memset_s(encryptString, ENCRYPTED_STRING_LENGTH + 1, 0, ENCRYPTED_STRING_LENGTH + 1);
    securec_check(rc, "\0", "\0");
    return defaultAuthMethod;
}

/*
 *	Scan the pre-parsed hba file, looking for a match to the port's connection
 *	request.
 */
static void check_hba(hbaPort* port)
{
    Oid roleid;
    ListCell* line = NULL;
    HbaLine* hba = NULL;
    bool isUsernameSame = false;
    bool isMatched = false;

    /* Get the target role's OID.  Note we do not error out for bad role. */
    roleid = get_role_oid(port->user_name, true);

#ifdef USE_IAM
    bool isIAM = false;
    if (roleid != InvalidOid) {
        isIAM = is_role_iamauth(roleid);
    }
#endif
    hba = (HbaLine*)palloc0(sizeof(HbaLine));
    int ret = pthread_rwlock_rdlock(&hba_rwlock);
    if (ret != 0) {
        pfree_ext(hba);
        ereport(ERROR,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("get hba rwlock failed when check hba config, return value:%d", ret)));
    }

    /* hba_rwlock will be released when ereport ERROR or FATAL. */
    PG_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    foreach (line, g_instance.libpq_cxt.comm_parsed_hba_lines) {
        /* the value of char* types in HbaLine will be copied to session memctx by copy_hba_line() in the end */
        errno_t rc = memcpy_s(hba, sizeof(HbaLine), lfirst(line), sizeof(HbaLine));
        securec_check(rc, "\0", "\0");
        /* Check connection type */
        if (hba->conntype == ctLocal) {
            /*
             * For initdb user, we should check if the connection request from
             * the installation enviroment.
             */
            if (roleid == INITIAL_USER_ID) {
                uid_t uid = 0;
                gid_t gid = 0;
                /* get the user id of the system, where client in */
                if (getpeereid(port->sock, &uid, &gid) != 0) {
                    pfree_ext(hba);
                    /* Provide special error message if getpeereid is a stub */
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Could not get user information on this platform")));
                }

                /*
                 * For connections from another coordinator, we could not
                 * get the userid. This case may also exist for other cases,
                 * like tools. (Usually happed when login with -h localhost
                 * or 127.0.0.1)
                 */
                if ((long)uid == USER_NULL_MASK) {
                    continue;
                }
                /*
                 * If the system username equals to the database username, continue
                 * the authorization without checking the usermap. If not, we should
                 * pay attentation to 'lateral ultra vires', which means, password is
                 * needed when login in in the non-installation enviroment under the
                 * same system user group.
                 */
                isUsernameSame= IsSysUsernameSameToDB(uid, port->user_name);
                if (!isUsernameSame && hba->auth_method == uaTrust) {
                    hba->auth_method = get_default_auth_method(port->user_name);
                }
            } else if (hba->auth_method == uaTrust || hba->auth_method == uaPeer) {
                /* For non-initdb user, password is always needed */
                hba->auth_method = get_default_auth_method(port->user_name);
            }

            if (!IS_AF_UNIX(port->raddr.addr.ss_family))
                continue;
        } else {
            if (IS_AF_UNIX(port->raddr.addr.ss_family))
                continue;

            /* Check SSL state */
#ifdef USE_SSL
            if (port->ssl != NULL) {
                /* Connection is SSL, match both "host" and "hostssl" */
                if (hba->conntype == ctHostNoSSL)
                    continue;
            } else {
                /* Connection is not SSL, match both "host" and "hostnossl" */
                if (hba->conntype == ctHostSSL)
                    continue;
            }
#else
            /* No SSL support, so reject "hostssl" lines */
            if (hba->conntype == ctHostSSL)
                continue;
#endif

            /* Check IP address */
            switch (hba->ip_cmp_method) {
                case ipCmpMask:
                    if (hba->hostname != NULL) {
                        if (!check_hostname(port, hba->hostname))
                            continue;
                    } else {
                        if (!check_ip(&port->raddr, (struct sockaddr*)&hba->addr, (struct sockaddr*)&hba->mask))
                            continue;
                    }
                    break;
                case ipCmpAll:
                    break;
                case ipCmpSameHost:
                case ipCmpSameNet:
                    if (!check_same_host_or_net(&port->raddr, hba->ip_cmp_method))
                        continue;
                    break;
                default:
                    /* shouldn't get here, but deem it no-match if so */
                    continue;
            }
        } /* != ctLocal */

        /* Check database and role */
        if (!check_db(port->database_name, port->user_name, roleid, hba->databases))
            continue;

        if (!check_role(port->user_name, roleid, hba->roles))
            continue;

        /* Found a record that matched and perform some security checks */
        if (hba->conntype != ctLocal) {
            /*
             * Generally, remote connections (i.e. logging with -h option) with trust mode are disallowed.
             *
             * However, there are a few exception scenarios as following:
             * 1 connections made by coordinators and inner maintenance tools, such as gs_rewind, cm_agent, etc.
             * -- trust mode is allowed, with an inner-cluster check done in process_startup_options
             * 2 local loop back connections
             * -- password is enforced
             * 3 connections of non-initial users within the cluster to coordinators
             * -- password is enforced
             */
            if (hba->auth_method == uaTrust) {
                if (IsConnPortFromCoord(port) || u_sess->proc_cxt.IsInnerMaintenanceTools) {
                    /* exception 1, just pass */
                } else if (IsLoopBackAddr(port)) {
                    /* exception 2, for local loop back connections, hba->remote_trust should be false */
                    hba->auth_method = get_default_auth_method(port->user_name);
                    hba->remoteTrust = false;
                } else if (roleid != INITIAL_USER_ID && IS_PGXC_COORDINATOR && is_cluster_internal_connection(port)) {
                    /*
                     * exception 3
                     * Since we cannot access pgxc_node until successful authentification,
                     * we might get an over-conservative inner-cluster judgement right after a restart.
                     * Nevertheless, this should not be a serious problem, because periodically
                     * local cm_agent connections will soon populate node information in shared memory.
                     */
                    hba->auth_method = get_default_auth_method(port->user_name);
                    hba->remoteTrust = false;
                } else {
                    ConnAuthMethodCorrect = false;
                    pfree_ext(hba);
                    ereport(FATAL,
                        (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                            errmsg("Forbid remote connection with trust method!")));
                }
            }
        }

#ifdef USE_IAM
        /* Change the inner auth method to IAM when using iam role with SHA256/MD5/SM3 to avoid confict. */
        if (isIAM && (hba->auth_method == uaSHA256 || hba->auth_method == uaMD5 || hba->auth_method == uaSM3)) {
            hba->auth_method = uaIAM;
        }
#endif
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
        /* For non-initial user, krb authentication is not allowed, sha256 req will be processed. */
        if (hba->auth_method == uaGSS && roleid != INITIAL_USER_ID && IsConnFromApp()) {
            hba->auth_method = get_default_auth_method(port->user_name);
        }
#endif
        isMatched = true;
        break;
    }

    /* If no matching entry was found, then implicitly reject. */
    if (!isMatched) {
        hba->auth_method = uaImplicitReject;
    }
    copy_hba_line(hba);
    port->hba = hba;
    PG_END_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    (void)pthread_rwlock_unlock(&hba_rwlock);
}

/*
 * Read the config file and create a List of HbaLine records for the contents.
 *
 * The configuration is read into a temporary list, and if any parse error
 * occurs the old list is kept in place and false is returned.	Only if the
 * whole file parses OK is the list replaced, and the function returns true.
 *
 * On a false result, caller will take care of reporting a FATAL error in case
 * this is the initial startup.  If it happens on reload, we just keep running
 * with the old data.
 */
bool load_hba(void)
{
    FILE* file = NULL;
    List* hba_lines = NIL;
    List* hba_line_nums = NIL;
    ListCell *line = NULL, *line_num = NULL;
    List* new_parsed_lines = NIL;
    bool ok = true;
    MemoryContext linecxt;
    MemoryContext oldcxt;
    MemoryContext hbacxt;

    file = AllocateFile(g_instance.attr.attr_common.HbaFileName, "r");
    if (file == NULL) {
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg("could not open configuration file \"%s\": %m", g_instance.attr.attr_common.HbaFileName)));
        return false;
    }

    linecxt = tokenize_file(g_instance.attr.attr_common.HbaFileName, file, &hba_lines, &hba_line_nums);
    (void)FreeFile(file);

    /* Now parse all the lines */
    hbacxt = AllocSetContextCreate(g_instance.instance_context,
        "hba parser context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    oldcxt = MemoryContextSwitchTo(hbacxt);
    forboth(line, hba_lines, line_num, hba_line_nums)
    {
        HbaLine* newline = NULL;

        if ((newline = parse_hba_line((List*)lfirst(line), lfirst_int(line_num))) == NULL) {
            /*
             * Parse error in the file, so indicate there's a problem.  NB: a
             * problem in a line will free the memory for all previous lines
             * as well!
             */
            MemoryContextReset(hbacxt);
            new_parsed_lines = NIL;
            ok = false;

            /*
             * Keep parsing the rest of the file so we can report errors on
             * more than the first row. Error has already been reported in the
             * parsing function, so no need to log it here.
             */
            continue;
        }

        new_parsed_lines = lappend(new_parsed_lines, newline);
    }

    /*
     * A valid HBA file must have at least one entry; else there's no way to
     * connect to the postmaster.  But only complain about this if we didn't
     * already have parsing errors.
     */
    if (ok && new_parsed_lines == NIL) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("configuration file \"%s\" contains no entries", g_instance.attr.attr_common.HbaFileName)));
        ok = false;
    }

    /* Free tokenizer memory */
    MemoryContextDelete(linecxt);
    MemoryContextSwitchTo(oldcxt);

    if (!ok) {
        /* File contained one or more errors, so bail out */
        MemoryContextDelete(hbacxt);
        return false;
    }

    /* Any ERROR will become PANIC errors when holding rwlock of hba_rwlock. */
    int ret = pthread_rwlock_wrlock(&hba_rwlock);
    if (ret == 0) {
        /* hba_rwlock will be released when ereport ERROR or FATAL. */
        PG_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
        /* Loaded new file successfully, replace the one we use */
        if (g_instance.libpq_cxt.parsed_hba_context != NULL) {
            MemoryContextDelete(g_instance.libpq_cxt.parsed_hba_context);
        }
        g_instance.libpq_cxt.parsed_hba_context = hbacxt;
        g_instance.libpq_cxt.comm_parsed_hba_lines = new_parsed_lines;
        PG_END_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
        (void)pthread_rwlock_unlock(&hba_rwlock);
        check_old_hba(false);
        return true;
    } else {
        MemoryContextDelete(hbacxt);
        ereport(WARNING,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("get hba rwlock failed when load hba config, return value:%d", ret)));
        return false;
    }
}

void check_old_hba(bool need_old_hba)
{
    struct stat st;
    char path[MAXPGPATH];
    int nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_hba.conf.old",
        g_instance.attr.attr_common.data_directory);
    securec_check_ss_c(nRet, "", "");

    if (stat(path, &st) != 0) {
        ereport(DEBUG5,
            (errcode_for_file_access(),
                errmsg("file does not exists \"%s\"", path)));
        return;
    }
    if (need_old_hba) {
        if (rename(path, g_instance.attr.attr_common.HbaFileName)) {
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg("failed to rename file \"%s\": %m", path)));
        }
    } else {
        if (unlink(path)) {
            ereport(LOG,
                (errcode_for_file_access(),
                    errmsg("failed to remove file \"%s\": %m", path)));
        }
    }
}

/*
 * Check whether the username actually matched or not 
 */
static void check_username_matched(bool case_insensitive, const char* regexp_pgrole, const char* pg_role, bool* found_p)
{
    if (case_insensitive) {
        if (pg_strcasecmp(regexp_pgrole, pg_role) == 0)
            *found_p = true;
    } else {
        if (strcmp(regexp_pgrole, pg_role) == 0)
            *found_p = true;
    }
}

/*
 *	Process one line from the ident config file.
 *
 *	Take the line and compare it to the needed map, pg_role and ident_user.
 *	*found_p and *error_p are set according to our results.
 */
static void parse_ident_usermap(List* line, int line_number, const char* usermap_name, const char* pg_role,
    const char* ident_user, bool case_insensitive, bool* found_p, bool* error_p)
{
    ListCell* field = NULL;
    List* tokens = NIL;
    HbaToken* token = NULL;
    char* file_map = NULL;
    char* file_pgrole = NULL;
    char* file_ident_user = NULL;

    *found_p = false;
    *error_p = false;

    Assert(line != NIL);
    field = list_head(line);

    /* Get the map token (must exist) */
    tokens = (List*)lfirst(field);
    IDENT_MULTI_VALUE(tokens);
    token = (HbaToken*)linitial(tokens);
    file_map = token->string;

    /* Get the ident user token */
    field = lnext(field);
    IDENT_FIELD_ABSENT(field);
    tokens = (List*)lfirst(field);
    IDENT_MULTI_VALUE(tokens);
    token = (HbaToken*)linitial(tokens);
    file_ident_user = token->string;

    /* Get the PG rolename token */
    field = lnext(field);
    IDENT_FIELD_ABSENT(field);
    tokens = (List*)lfirst(field);
    IDENT_MULTI_VALUE(tokens);
    token = (HbaToken*)linitial(tokens);
    file_pgrole = token->string;

    if (strcmp(file_map, usermap_name) != 0)
        /* Line does not match the map name we're looking for, so just abort */
        return;

    /* Match? */
    if (file_ident_user[0] == '/') {
        /*
         * When system username starts with a slash, treat it as a regular
         * expression. In this case, we process the system username as a
         * regular expression that returns exactly one match. This is replaced
         * for \1 in the database username string, if present.
         */
        int r;
        regex_t re;
        regmatch_t matches[2];
        pg_wchar* wstr = NULL;
        int wlen;
        char* ofs = NULL;
        char* regexp_pgrole = NULL;

        wstr = (pg_wchar*)palloc((strlen(file_ident_user + 1) + 1) * sizeof(pg_wchar));
        wlen = pg_mb2wchar_with_len(file_ident_user + 1, wstr, strlen(file_ident_user + 1));

        /*
         * XXX: Major room for optimization: regexps could be compiled when
         * the file is loaded and then re-used in every connection.
         */
        r = pg_regcomp(&re, wstr, wlen, REG_ADVANCED, C_COLLATION_OID);
        if (r) {
            char errstr[100];

            pg_regerror(r, &re, errstr, sizeof(errstr));
            ereport(LOG,
                (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                    errmsg("invalid regular expression \"%s\": %s", file_ident_user + 1, errstr)));

            pfree_ext(wstr);
            *error_p = true;
            return;
        }
        pfree_ext(wstr);

        size_t wstr_size = (strlen(ident_user) + 1) * sizeof(pg_wchar);
        if (wstr_size/sizeof(pg_wchar) != ((strlen(ident_user) + 1))) {
            ereport(ERROR,
                (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                    errmsg("palloc size overflow")));
        }

        wstr = (pg_wchar*)palloc(wstr_size);
        wlen = pg_mb2wchar_with_len(ident_user, wstr, strlen(ident_user));

        r = pg_regexec(&re, wstr, wlen, 0, NULL, 2, matches, 0);
        if (r) {
            char errstr[100] = {0};

            if (r != REG_NOMATCH) {
                /* REG_NOMATCH is not an error, everything else is */
                pg_regerror(r, &re, errstr, sizeof(errstr));
                ereport(LOG,
                    (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        errmsg("regular expression match for \"%s\" failed: %s", file_ident_user + 1, errstr)));
                *error_p = true;
            }

            pfree_ext(wstr);
            pg_regfree(&re);
            return;
        }
        pfree_ext(wstr);

        if ((ofs = strstr(file_pgrole, "\\1")) != NULL) {
            /* substitution of the first argument requested */
            if (matches[1].rm_so < 0) {
                ereport(LOG,
                    (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                        errmsg(
                            "regular expression \"%s\" has no subexpressions as requested by backreference in \"%s\"",
                            file_ident_user + 1,
                            file_pgrole)));
                pg_regfree(&re);
                *error_p = true;
                return;
            }

            /*
             * length: original length minus length of \1 plus length of match
             * plus null terminator
             */
            int length = strlen(file_pgrole) - 2 + (matches[1].rm_eo - matches[1].rm_so) + 1;
            regexp_pgrole = (char*)palloc0(length);
            errno_t rc = strncpy_s(regexp_pgrole, length, file_pgrole, (ofs - file_pgrole));
            securec_check(rc, regexp_pgrole, "\0");
            rc = memcpy_s(regexp_pgrole + strlen(regexp_pgrole),
                length - strlen(regexp_pgrole),
                ident_user + matches[1].rm_so,
                matches[1].rm_eo - matches[1].rm_so);
            securec_check(rc, regexp_pgrole, "\0");
            rc = strcat_s(regexp_pgrole, length, ofs + 2);
            securec_check(rc, regexp_pgrole, "\0");
        } else {
            /* no substitution, so copy the match */
            regexp_pgrole = pstrdup(file_pgrole);
        }

        pg_regfree(&re);

        /*
         * now check if the username actually matched what the user is trying
         * to connect as
         */
        check_username_matched(case_insensitive, regexp_pgrole, pg_role, found_p);
        pfree_ext(regexp_pgrole);
        return;
    } else {
        /* Not regular expression, so make complete match */
        if (case_insensitive) {
            if (pg_strcasecmp(file_pgrole, pg_role) == 0 && pg_strcasecmp(file_ident_user, ident_user) == 0)
                *found_p = true;
        } else {
            if (strcmp(file_pgrole, pg_role) == 0 && strcmp(file_ident_user, ident_user) == 0)
                *found_p = true;
        }
    }
    return;
}

/*
 *	Scan the (pre-parsed) ident usermap file line by line, looking for a match
 *
 *	See if the user with ident username "auth_user" is allowed to act
 *	as openGauss user "pg_role" according to usermap "usermap_name".
 *
 *	Special case: Usermap NULL, equivalent to what was previously called
 *	"sameuser" or "samerole", means don't look in the usermap file.
 *	That's an implied map wherein "pg_role" must be identical to
 *	"auth_user" in order to be authorized.
 *
 *	Iff authorized, return STATUS_OK, otherwise return STATUS_ERROR.
 */
int check_usermap(const char* usermap_name, const char* pg_role, const char* auth_user, bool case_insensitive)
{
    bool found_entry = false;
    bool error = false;

    if (usermap_name == NULL || usermap_name[0] == '\0') {
        if (case_insensitive) {
            if (pg_strcasecmp(pg_role, auth_user) == 0)
                return STATUS_OK;
        } else {
            if (strcmp(pg_role, auth_user) == 0)
                return STATUS_OK;
        }
        ereport(
            LOG, (errmsg("provided user name (%s) and authenticated user name (%s) do not match", pg_role, auth_user)));
        return STATUS_ERROR;
    } else {
        ListCell *line_cell = NULL, *num_cell = NULL;

        forboth(line_cell, u_sess->libpq_cxt.ident_lines, num_cell, u_sess->libpq_cxt.ident_line_nums)
        {
            parse_ident_usermap((List*)lfirst(line_cell),
                lfirst_int(num_cell),
                usermap_name,
                pg_role,
                auth_user,
                case_insensitive,
                &found_entry,
                &error);
            if (found_entry || error)
                break;
        }
    }
    if (!found_entry && !error) {
        ereport(LOG,
            (errmsg("no match in usermap \"%s\" for user \"%s\" authenticated as \"%s\"",
                usermap_name,
                pg_role,
                auth_user)));
    }
    return found_entry ? STATUS_OK : STATUS_ERROR;
}

/*
 * @Description	: Read the ident config file and populate ident_lines and
 * 				  ident_line_nums. Like parsed_hba_lines, ident_lines is a
 *				  triple-nested List of lines, fields and tokens.
 * @NOTE		: It is ok to continue if we fail to load the IDENT file,
 *				  although ig means that we do not exist any authentication
 *				  mapping between sys_user and database user. So we do not
 *				  want any return value.
 */
void load_ident(void)
{
    FILE* file = NULL;

    if (u_sess->libpq_cxt.ident_context != NULL) {
        MemoryContextDelete(u_sess->libpq_cxt.ident_context);
        u_sess->libpq_cxt.ident_context = NULL;
    }

    file = AllocateFile(g_instance.attr.attr_common.IdentFileName, "r");
    if (file == NULL) {
        /* not fatal ... we just won't do any special ident maps */
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg("could not open usermap file \"%s\": %m", g_instance.attr.attr_common.IdentFileName)));
    } else {
        u_sess->libpq_cxt.ident_context = tokenize_file(g_instance.attr.attr_common.IdentFileName,
            file,
            &u_sess->libpq_cxt.ident_lines,
            &u_sess->libpq_cxt.ident_line_nums);
        (void)FreeFile(file);
    }
}

/*
 *	Determine what authentication method should be used when accessing database
 *	"database" from frontend "raddr", user "user".	Return the method and
 *	an optional argument (stored in fields of *port), and STATUS_OK.
 *
 *	If the file does not contain any entry matching the request, we return
 *	method = uaImplicitReject.
 */
void hba_getauthmethod(hbaPort* port)
{
    /*
     * We may receive replication request from gs_basebackup, in this case, use check_hba
     * to verify. But if we run gs_basebackup at the machine in ReplConnInfo, we can't
     * distinguish whether this request is from a standby process or gs_basebackup...
     * In this case, we still need to use check_hba_replication for compatibility.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (IsDSorHaWalSender() ) {
#else        
    if (IsDSorHaWalSender() && (is_node_internal_connection(port) || AM_WAL_HADR_SENDER)) {
#endif        
        check_hba_replication(port);
    } else {
        check_hba(port);
    }
}

/*
 *	Check if the current connection is a node internal connection
 */
bool is_node_internal_connection(hbaPort* port)
{
    const SockAddr remote_addr = port->raddr;
    const SockAddr local_addr = port->laddr;
    char remote_host[NI_MAXHOST] = {0};
    char local_host[NI_MAXHOST] = {0};

    remote_host[0] = '\0';
    local_host[0] = '\0';

    (void)pg_getnameinfo_all(&remote_addr.addr,
        remote_addr.salen,
        remote_host,
        sizeof(remote_host),
        NULL,
        0,
        NI_NUMERICHOST | NI_NUMERICSERV);
    (void)pg_getnameinfo_all(
        &local_addr.addr, local_addr.salen, local_host, sizeof(local_host), NULL, 0, NI_NUMERICHOST | NI_NUMERICSERV);

    if (strlen(remote_host) == 0) {
        return false;
    }

    if (strcmp(remote_host, local_host) == 0) {
        ereport(DEBUG2, (errmsg("remote host is:%s and local host is:%s", remote_host, local_host)));
        return true;
    }

    for (int i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
        char *replconninfo = NULL;
        if (i >= MAX_REPLNODE_NUM) {
            replconninfo = u_sess->attr.attr_storage.CrossClusterReplConnInfoArr[i - MAX_REPLNODE_NUM];
        } else {
            replconninfo = u_sess->attr.attr_storage.ReplConnInfoArr[i];
        }
        if (replconninfo && *replconninfo != '\0' && (strcasestr(replconninfo, remote_host) != NULL ||
            strcasestr(replconninfo, port->remote_host) != NULL)) {
            ereport(DEBUG2, (errmsg("remote host is:%s in replconninfo %s", remote_host, replconninfo)));
            return true;
        }
    }

    return false;
}

/*
 *	Check if the current connection is a cluster internal connection including
 *	a local connection or a CN-CN/CN-DN connection within the cluster.
 *  NOTE: This function can only be used on CN.
 */
bool is_cluster_internal_connection(hbaPort* port)
{
    const SockAddr remote_addr = port->raddr;
    const SockAddr local_addr = port->laddr;
    char remote_host[NI_MAXHOST];
    char local_host[NI_MAXHOST];
    int i;

    remote_host[0] = '\0';
    local_host[0] = '\0';

    (void)pg_getnameinfo_all(&remote_addr.addr,
        remote_addr.salen,
        remote_host,
        sizeof(remote_host),
        NULL,
        0,
        NI_NUMERICHOST | NI_NUMERICSERV);
    (void)pg_getnameinfo_all(
        &local_addr.addr, local_addr.salen, local_host, sizeof(local_host), NULL, 0, NI_NUMERICHOST | NI_NUMERICSERV);

    if (strcmp(remote_host, local_host) == 0 && strlen(remote_host) != 0) {
        ereport(DEBUG2, (errmsg("remote host is:%s and local host is:%s", remote_host, local_host)));
        return true;
    }

    LWLockAcquire(NodeTableLock, LW_SHARED);
    /* Check whether the app client is on the machine as coordinator in cluster. */
    for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoordsInCluster; i++) {
        if (strcmp(remote_host, t_thrd.pgxc_cxt.coDefsInCluster[i].nodehost.data) == 0) {
            ereport(DEBUG2,
                (errmsg("remote host is:%s and inner ip is:%s",
                    remote_host,
                    t_thrd.pgxc_cxt.coDefsInCluster[i].nodehost.data)));
            LWLockRelease(NodeTableLock);
            return true;
        }
    }

    /* Check whether the app client is on the machine as datanode in cluster. */
    for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++) {
        if (strcmp(remote_host, t_thrd.pgxc_cxt.dnDefs[i].nodehost.data) == 0 ||
            strcmp(remote_host, t_thrd.pgxc_cxt.dnDefs[i].nodehost1.data) == 0) {
            ereport(DEBUG2,
                (errmsg("remote host is:%s and inner ip is:%s-%s",
                    remote_host,
                    t_thrd.pgxc_cxt.dnDefs[i].nodehost.data,
                    t_thrd.pgxc_cxt.dnDefs[i].nodehost1.data)));
            LWLockRelease(NodeTableLock);
            return true;
        }
    }

    /* Check whether the app client is on the machine as standby datanode in cluster. */
    if (IS_DN_MULTI_STANDYS_MODE()) {
        for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes; i++) {
            if (strcmp(remote_host, t_thrd.pgxc_cxt.dnStandbyDefs[i].nodehost.data) == 0 ||
                strcmp(remote_host, t_thrd.pgxc_cxt.dnStandbyDefs[i].nodehost1.data) == 0) {
                ereport(DEBUG2,
                    (errmsg("remote host is:%s and inner ip is:%s-%s",
                        remote_host,
                        t_thrd.pgxc_cxt.dnStandbyDefs[i].nodehost.data,
                        t_thrd.pgxc_cxt.dnStandbyDefs[i].nodehost1.data)));
                LWLockRelease(NodeTableLock);
                return true;
            }
        }
    }

    LWLockRelease(NodeTableLock);
    return false;
}

bool is_cluster_internal_IP(sockaddr peer_addr)
{
    /* convert to sockaddr_in struct format. */
    sockaddr_in* pSin = (sockaddr_in*)&peer_addr;
    const int MAX_IP_ADDRESS_LEN = 64;
    char ipstr[MAX_IP_ADDRESS_LEN] = {'\0'};
    bool isInternalIP = false;

    inet_ntop(AF_INET, &pSin->sin_addr, ipstr, MAX_IP_ADDRESS_LEN - 1);
    if (strcmp(ipstr, "127.0.0.1") == 0 || strcmp(ipstr, "localhost") == 0)
        return true;

    if (g_instance.libpq_cxt.comm_parsed_hba_lines == NIL) {
        fprintf(stdout, "comm_parsed_hba_lines is NIL");
        (void)fflush(stdout);
        return false;
    }

    // construct a "SockAddr" structure
    SockAddr* raddr = (SockAddr*)malloc(sizeof(SockAddr));
    if (raddr == NULL)
        return false;

    int rc = memset_s(raddr, sizeof(SockAddr), 0, sizeof(SockAddr));
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(&raddr->addr, sizeof(sockaddr_storage), &peer_addr, sizeof(sockaddr));
    securec_check(rc, "\0", "\0");

    ListCell* line = NULL;
    HbaLine* hba = NULL;

    int ret = pthread_rwlock_rdlock(&hba_rwlock);
    if (ret != 0) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("get hba rwlock failed when check cluster internal ip, return value:%d", ret)));
        free(raddr);
        return false;
    }

    /* hba_rwlock will be released when ereport ERROR or FATAL. */
    PG_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    foreach (line, g_instance.libpq_cxt.comm_parsed_hba_lines) {
        hba = (HbaLine*)lfirst(line);
        /* check connection type */
        if (hba->auth_method == uaTrust || hba->auth_method == uaGSS) {
            if (check_ip(raddr, (struct sockaddr*)&hba->addr, (struct sockaddr*)&hba->mask)) {
                isInternalIP = true;
                break;
            }
        }
    }

    PG_END_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    (void)pthread_rwlock_unlock(&hba_rwlock);
    free(raddr);
    return isInternalIP;
}

/* Check whether the ip is configured in pg_hba.conf */
bool check_ip_whitelist(hbaPort* port, char* ip, unsigned int ip_len)
{
    Assert(port != NULL);
    const SockAddr remote_addr = port->raddr;
    const SockAddr local_addr = port->laddr;
    char remote_host[NI_MAXHOST];
    char local_host[NI_MAXHOST];
    bool isWhitelistIP = false;

    remote_host[0] = '\0';
    local_host[0] = '\0';

    (void)pg_getnameinfo_all(&remote_addr.addr,
        remote_addr.salen,
        remote_host,
        sizeof(remote_host),
        NULL,
        0,
        NI_NUMERICHOST | NI_NUMERICSERV);
    (void)pg_getnameinfo_all(
        &local_addr.addr, local_addr.salen, local_host, sizeof(local_host), NULL, 0, NI_NUMERICHOST | NI_NUMERICSERV);
    errno_t rc = strncpy_s(ip, ip_len, remote_host, ip_len - 1);
    securec_check(rc, "\0", "\0");

    if (strcmp(remote_host, local_host) == 0 && strlen(remote_host) != 0) {
        return true;
    }

    ListCell* line = NULL;
    HbaLine* hba = NULL;
    int ret = pthread_rwlock_rdlock(&hba_rwlock);
    if (ret != 0) {
        ereport(LOG,
            (errcode(ERRCODE_CONFIG_FILE_ERROR),
                errmsg("get hba rwlock failed when check ip whitelist, return value:%d", ret)));
        return false;
    }

    /* hba_rwlock will be released when ereport ERROR or FATAL. */
    PG_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    foreach (line, g_instance.libpq_cxt.comm_parsed_hba_lines) {
        hba = (HbaLine*)lfirst(line);
        if (hba->auth_method != uaReject) {
            if (check_ip(&port->raddr, (struct sockaddr*)&hba->addr, (struct sockaddr*)&hba->mask)) {
                isWhitelistIP = true;
                break;
            }
        }
    }

    PG_END_ENSURE_ERROR_CLEANUP(hba_rwlock_cleanup, (Datum)0);
    (void)pthread_rwlock_unlock(&hba_rwlock);
    return isWhitelistIP;
}

