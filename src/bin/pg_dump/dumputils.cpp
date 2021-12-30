/* -------------------------------------------------------------------------
 *
 * Utility routines for SQL dumping
 *	Basically this is stuff that is useful in both pg_dump and pg_dumpall.
 *	Lately it's also being used by psql and bin/scripts/ ...
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumputils.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include "dumputils.h"
#include "dumpmem.h"
#include "pgtime.h"
#include "parser/keywords.h"

#include "bin/elog.h"
#ifdef WIN32_PG_DUMP
#include "windows.h"
#include "time.h"
#include "flock.h"
#endif

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

/* Globals from keywords.c */
extern ScanKeyword FEScanKeywords[];
extern int NumFEScanKeywords;

/* Globals exported by this file */
int quote_all_identifiers = 0;
const char* progname = NULL;
const char* dbname = NULL;
const char* instport = NULL;

char* binary_upgrade_oldowner = NULL;
char* binary_upgrade_newowner = NULL;

#define MAX_ON_EXIT_NICELY 20

static struct {
    on_exit_nicely_callback function;
    void* arg;
} on_exit_nicely_list[MAX_ON_EXIT_NICELY];

static int on_exit_nicely_index;
static int lock_fd = -1;

#define supports_grant_options(version) ((version) >= 70400)

static bool parseAclItem(const char* item, const char* type, const char* name, const char* subname, int remoteVersion,
    PQExpBuffer grantee, PQExpBuffer grantor, PQExpBuffer privs, PQExpBuffer privswgo);
static char* copyAclUserName(PQExpBuffer output, const char* input);
static void AddAcl(PQExpBuffer aclbuf, const char* keyword, const char* subname);

#ifdef WIN32
static bool parallel_init_done = false;
static DWORD tls_index;
static DWORD mainThreadId;
#endif

void init_parallel_dump_utils(void)
{
#ifdef WIN32
    if (!parallel_init_done) {
        tls_index = TlsAlloc();
        parallel_init_done = true;
        mainThreadId = GetCurrentThreadId();
    }
#endif
}

/*
 *	Quotes input string if it's not a legitimate SQL identifier as-is.
 *
 *	Note that the returned string must be used before calling fmtId again,
 *	since we re-use the same return buffer each time.  Non-reentrant but
 *	reduces memory leakage. (On Windows the memory leakage will be one buffer
 *	per thread, which is at least better than one per call).
 */
const char* fmtId(const char* rawid)
{
    /*
     * The Tls code goes awry if we use a static var, so we provide for both
     * static and auto, and omit any use of the static var when using Tls.
     */
    static PQExpBuffer s_id_return = NULL;
    PQExpBuffer id_return;

    const char* cp = rawid;
    bool need_quotes = false;

#ifdef WIN32
    if (parallel_init_done)
        id_return = (PQExpBuffer)TlsGetValue(tls_index); /* 0 when not set */
    else
        id_return = s_id_return;
#else
    id_return = s_id_return;
#endif

    if (id_return != NULL) /* first time through? */
    {
        /* same buffer, just wipe contents */
        (void)resetPQExpBuffer(id_return);
    } else {
        /* new buffer */
        id_return = createPQExpBuffer();
#ifdef WIN32
        if (parallel_init_done)
            TlsSetValue(tls_index, id_return);
        else
            s_id_return = id_return;
#else
        s_id_return = id_return;
#endif
    }

    /*
     * These checks need to match the identifier production in scan.l. Don't
     * use islower() etc.
     */
    if (quote_all_identifiers) {
        need_quotes = true;
    } else if (!((rawid[0] >= 'a' && rawid[0] <= 'z') || rawid[0] == '_')) { /* slightly different rules for first character */
        need_quotes = true;
    } else {
        /* otherwise check the entire string */
        for (cp = rawid; *cp; cp++) {
            if (!((*cp >= 'a' && *cp <= 'z') || (*cp >= '0' && *cp <= '9') || (*cp == '_'))) {
                need_quotes = true;
                break;
            }
        }
    }

    if (!need_quotes) {
        /*
         * Check for keyword.  We quote keywords except for unreserved ones.
         * (In some cases we could avoid quoting a col_name or type_func_name
         * keyword, but it seems much harder than it's worth to tell that.)
         *
         * Note: ScanKeywordLookup() does case-insensitive comparison, but
         * that's fine, since we already know we have all-lower-case.
         */
        const ScanKeyword* keyword = ScanKeywordLookup(rawid, FEScanKeywords, NumFEScanKeywords);

        if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
            need_quotes = true;
    }

    if (!need_quotes) {
        /* no quoting needed */
        (void)appendPQExpBufferStr(id_return, rawid);
    } else {
        (void)appendPQExpBufferChar(id_return, '\"');
        for (cp = rawid; *cp; cp++) {
            /*
             * Did we find a double-quote in the string? Then make this a
             * double double-quote per SQL99. Before, we put in a
             * backslash/double-quote pair. - thomas 2000-08-05
             */
            if (*cp == '\"')
                (void)appendPQExpBufferChar(id_return, '\"');
            (void)appendPQExpBufferChar(id_return, *cp);
        }
        (void)appendPQExpBufferChar(id_return, '\"');
    }

    return id_return->data;
}

/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  We assume the specified client_encoding and
 * standard_conforming_strings settings.
 *
 * This is essentially equivalent to libpq's PQescapeStringInternal,
 * except for the output buffer structure.	We need it in situations
 * where we do not have a PGconn available.  Where we do,
 * appendStringLiteralConn is a better choice.
 */
void appendStringLiteral(PQExpBuffer buf, const char* str, int encoding, bool std_strings)
{
    size_t length = strlen(str);
    const char* source = str;
    char* target = NULL;

    if (!enlargePQExpBuffer(buf, 2 * length + 2))
        return;

    target = buf->data + buf->len;
    *target++ = '\'';

    while (*source != '\0') {
        char c = *source;
        int len;
        int i;

        /* Fast path for plain ASCII */
        if (!IS_HIGHBIT_SET(c)) {
            /* Apply quoting if needed */
            if (SQL_STR_DOUBLE(c, !std_strings))
                *target++ = c;
            /* Copy the character */
            *target++ = c;
            source++;
            continue;
        }

        /* Slow path for possible multibyte characters */
        len = PQmblen(source, encoding);

        /* Copy the character */
        for (i = 0; i < len; i++) {
            if (*source == '\0') {
                break;
            }
            *target++ = *source++;
        }

        /*
         * If we hit premature end of string (ie, incomplete multibyte
         * character), try to pad out to the correct length with spaces. We
         * may not be able to pad completely, but we will always be able to
         * insert at least one pad space (since we'd not have quoted a
         * multibyte character).  This should be enough to make a string that
         * the server will error out on.
         */
        if (i < len) {
            char* stop = buf->data + buf->maxlen - 2;

            for (; i < len; i++) {
                if (target >= stop) {
                    break;
                }
                *target++ = ' ';
            }
            break;
        }
    }

    /* Write the terminating quote and NUL character. */
    *target++ = '\'';
    *target = '\0';

    buf->len = target - buf->data;
}

/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  Encoding and string syntax rules are as indicated
 * by current settings of the PGconn.
 */
void appendStringLiteralConn(PQExpBuffer buf, const char* str, PGconn* conn)
{
    size_t length = strlen(str);

    /*
     * XXX This is a kluge to silence escape_string_warning in our utility
     * programs.  It should go away someday.
     */
    if (strchr(str, '\\') != NULL && PQserverVersion(conn) >= 80100) {
        /* ensure we are not adjacent to an identifier */
        if (buf->len > 0 && buf->data[buf->len - 1] != ' ')
            (void)appendPQExpBufferChar(buf, ' ');
        (void)appendPQExpBufferChar(buf, ESCAPE_STRING_SYNTAX);
        appendStringLiteral(buf, str, PQclientEncoding(conn), false);
        return;
    } /* XXX end kluge */

    if (!enlargePQExpBuffer(buf, 2 * length + 2))
        return;
    (void)appendPQExpBufferChar(buf, '\'');
    buf->len += PQescapeStringConn(conn, buf->data + buf->len, str, length, NULL);
    (void)appendPQExpBufferChar(buf, '\'');
}

/*
 * Convert a string value to a dollar quoted literal and append it to
 * the given buffer. If the dqprefix parameter is not NULL then the
 * dollar quote delimiter will begin with that (after the opening $).
 *
 * No escaping is done at all on str, in compliance with the rules
 * for parsing dollar quoted strings.  Also, we need not worry about
 * encoding issues.
 */
void appendStringLiteralDQ(PQExpBuffer buf, const char* str, const char* dqprefix)
{
    static const char suffixes[] = "_XXXXXXX";
    int nextchar = 0;
    PQExpBuffer delimBuf = createPQExpBuffer();

    /* start with $ + dqprefix if not NULL */
    (void)appendPQExpBufferChar(delimBuf, '$');
    if (dqprefix != NULL)
        (void)appendPQExpBufferStr(delimBuf, dqprefix);

    /*
     * Make sure we choose a delimiter which (without the trailing $) is not
     * present in the string being quoted. We don't check with the trailing $
     * because a string ending in $foo must not be quoted with $foo$.
     */
    while (strstr(str, delimBuf->data) != NULL) {
        (void)appendPQExpBufferChar(delimBuf, suffixes[nextchar++]);
        nextchar %= sizeof(suffixes) - 1;
    }

    /* add trailing $ */
    (void)appendPQExpBufferChar(delimBuf, '$');

    /* quote it and we are all done */
    (void)appendPQExpBufferStr(buf, delimBuf->data);
    (void)appendPQExpBufferStr(buf, str);
    (void)appendPQExpBufferStr(buf, delimBuf->data);

    (void)destroyPQExpBuffer(delimBuf);
}

/*
 * Convert a bytea value (presented as raw bytes) to an SQL string literal
 * and append it to the given buffer.  We assume the specified
 * standard_conforming_strings setting.
 *
 * This is needed in situations where we do not have a PGconn available.
 * Where we do, PQescapeByteaConn is a better choice.
 */
void appendByteaLiteral(PQExpBuffer buf, const unsigned char* str, size_t length, bool std_strings)
{
    const unsigned char* source = str;
    char* target = NULL;

    static const char hextbl[] = "0123456789abcdef";

    /*
     * This implementation is hard-wired to produce hex-format output. We do
     * not know the server version the output will be loaded into, so making
     * an intelligent format choice is impossible.	It might be better to
     * always use the old escaped format.
     */
    if (!enlargePQExpBuffer(buf, 2 * length + 5))
        return;

    target = buf->data + buf->len;
    *target++ = '\'';
    if (!std_strings) {
        *target++ = '\\';
    }
    *target++ = '\\';
    *target++ = 'x';

    while (length-- > 0) {
        unsigned char c = *source++;

        *target++ = hextbl[(c >> 4) & 0xF];
        *target++ = hextbl[c & 0xF];
    }

    /* Write the terminating quote and NUL character. */
    *target++ = '\'';
    *target = '\0';

    buf->len = target - buf->data;
}

/*
 * Convert backend's version string into a number.
 */
int parse_version(const char* versionString)
{
    int vmaj = 0;
    int vmin = 0;
    int vrev = 0;

    int cnt = sscanf_s(versionString, "%d.%d.%d", &vmaj, &vmin, &vrev);
    if (cnt < 2) {
        return -1;
    }

    if (cnt == 2) {
        vrev = 0;
    }

    return (100 * vmaj + vmin) * 100 + vrev;
}

/*
 * Deconstruct the text representation of a 1-dimensional openGauss array
 * into individual items.
 *
 * On success, returns true and sets *itemarray and *nitems to describe
 * an array of individual strings.	On parse failure, returns false;
 * *itemarray may exist or be NULL.
 *
 * NOTE: free'ing itemarray is sufficient to deallocate the working storage.
 */
bool parsePGArray(const char* atext, char*** itemarray, int* nitems)
{
    int inputlen;
    char** items;
    char* strings = NULL;
    int curitem;

    /*
     * We expect input in the form of "{item,item,item}" where any item is
     * either raw data, or surrounded by double quotes (in which case embedded
     * characters including backslashes and quotes are backslashed).
     *
     * We build the result as an array of pointers followed by the actual
     * string data, all in one malloc block for convenience of deallocation.
     * The worst-case storage need is not more than one pointer and one
     * character for each input character (consider "{,,,,,,,,,,}").
     */
    *itemarray = NULL;
    *nitems = 0;
    inputlen = strlen(atext);
    if (inputlen < 2 || atext[0] != '{' || atext[inputlen - 1] != '}') {
        return false; /* bad input */
    }
    items = (char**)pg_malloc(inputlen * (sizeof(char*) + sizeof(char)));
    if (items == NULL) {
        return false; /* out of memory */
    }
    *itemarray = items;
    strings = (char*)(items + inputlen);

    atext++; /* advance over initial '{' */
    curitem = 0;
    while (*atext != '}') {
        if (*atext == '\0') {
            return false; /* premature end of string */
        }
        items[curitem] = strings;
        while (*atext != '}' && *atext != ',') {
            if (*atext == '\0') {
                return false; /* premature end of string */
            }
            if (*atext != '"') {
                *strings++ = *atext++; /* copy unquoted data */
            }
            else {
                /* process quoted substring */
                atext++;
                while (*atext != '"') {
                    if (*atext == '\0') {
                        return false; /* premature end of string */
                    }
                    if (*atext == '\\') {
                        atext++;
                        if (*atext == '\0') {
                            return false; /* premature end of string */
                        }
                    }
                    *strings++ = *atext++; /* copy quoted data */
                }
                atext++;
            }
        }
        *strings++ = '\0';
        if (*atext == ',') {
            atext++;
        }
        curitem++;
    }
    if (atext[1] != '\0') {
        return false; /* bogus syntax (embedded '}') */
    }
    *nitems = curitem;
    return true;
}

/* If you use gs_strdup, it will cause gsql dependencies to change. So another alias to use. */
char* dumpStrdup(const char* string)
{
    char* result = NULL;

    if (string == NULL)
        exit_horribly(NULL, "cannot duplicate null pointer\n");

    result = strdup(string);
    if (result == NULL)
        exit_horribly(NULL, "out of memory\n");

    return result;
}

/*
 * getRolePassword:
 *    input: PGconn and usename
 *    output: rolepassword
 * Through the user name to obtain the user's encrypted password
 */
char* getRolePassword(PGconn* conn, const char* user)
{
    PQExpBuffer buf = createPQExpBuffer();
    PQExpBuffer result = createPQExpBuffer();
    PGresult* res = NULL;
    char* tmp = NULL;
    char* rolepassword = NULL;

    /* Each user name in the pg_authid is unique */
    appendPQExpBuffer(buf, "SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = '%s';", user);
    res = PQexec(conn, buf->data);
    if (PQntuples(res) != 1) {
        write_msg(NULL, "query to get role password for role \"%s\" failed: wrong number of rows returned\n", user);
        PQclear(res);
        destroyPQExpBuffer(result);
        destroyPQExpBuffer(buf);
        exit_nicely(1);
    }

    int i_rolpassword = PQfnumber(res, "rolpassword");
    tmp = dumpStrdup(PQgetvalue(res, 0, i_rolpassword));
    /* The result string contains a line feed, so it needs to be processed again */
    (void)appendStringLiteralConn(result, tmp, conn);
    rolepassword = dumpStrdup(result->data);

    free(tmp);
    tmp = NULL;
    PQclear(res);
    destroyPQExpBuffer(result);
    destroyPQExpBuffer(buf);

    return rolepassword;
}

/*
 * Build GRANT/REVOKE command(s) for an object.
 *
 *	name: the object name, in the form to use in the commands (already quoted)
 *	subname: the sub-object name, if any (already quoted); NULL if none
 *	type: the object type (as seen in GRANT command: must be one of
 *		TABLE, SEQUENCE, FUNCTION, LANGUAGE, SCHEMA, DATABASE, TABLESPACE,
 *		FOREIGN DATA WRAPPER, SERVER, DATA SOURCE or LARGE OBJECT)
 *	acls: the ACL string fetched from the database
 *	owner: username of object owner (will be passed through fmtId); can be
 *		NULL or empty string to indicate "no owner known"
 *	prefix: string to prefix to each generated command; typically empty
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 *
 * Note: when processing a default ACL, prefix is "ALTER DEFAULT PRIVILEGES "
 * or something similar, and name is an empty string.
 *
 * Note: beware of passing a fmtId() result directly as 'name' or 'subname',
 * since this routine uses fmtId() internally.
 */
bool buildACLCommands(const char* name, const char* subname, const char* type, const char* acls, const char* owner,
    const char* prefix, int remoteVersion, PQExpBuffer sql, PGconn* conn)
{
    char** aclitems = NULL;
    int naclitems = 0;
    int i = 0;
    PQExpBuffer grantee, grantor, privs, privswgo;
    PQExpBuffer firstsql, secondsql;
    bool found_owner_privs = false;
    bool hasRevoke = false;
    char* rolepassword = NULL;
    char* newname = NULL;

    if (strlen(acls) == 0)
        return true; /* object has default permissions */

    /* treat empty-string owner same as NULL */
    if ((owner != NULL) && *owner == '\0')
        owner = NULL;

    if (!parsePGArray(acls, &aclitems, &naclitems)) {
        if (aclitems != NULL) {
            free(aclitems);
            aclitems = NULL;
        }
        return false;
    }

    grantee = createPQExpBuffer();
    grantor = createPQExpBuffer();
    privs = createPQExpBuffer();
    privswgo = createPQExpBuffer();

    /*
     * At the end, these two will be pasted together to form the result. But
     * the owner privileges need to go before the other ones to keep the
     * dependencies valid.	In recent versions this is normally the case, but
     * in old versions they come after the PUBLIC privileges and that results
     * in problems if we need to run REVOKE on the owner privileges.
     */
    firstsql = createPQExpBuffer();
    secondsql = createPQExpBuffer();

    newname = (char*)pg_malloc(strlen(name) + 3);
    int rc = memset_s(newname, (strlen(name) + 3), 0, strlen(name) + 3);
    securec_check_c(rc, newname, "\0");

    if (remoteVersion >= 90200 && strcmp(type, "NODE GROUP") == 0) {
        rc = snprintf_s(newname, strlen(name) + 3, strlen(name) + 2, "\"%s\"", name);
    } else {
        rc = snprintf_s(newname, strlen(name) + 3, strlen(name) + 2, "%s", name);
    }
    securec_check_ss_c(rc, newname, "\0");
    /*
     * Always start with REVOKE ALL FROM PUBLIC, so that we don't have to
     * wire-in knowledge about the default public privileges for different
     * kinds of objects.
     */
    (void)appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
    if (NULL != subname)
        (void)appendPQExpBuffer(firstsql, "(%s)", subname);
    (void)appendPQExpBuffer(firstsql, " ON %s %s FROM PUBLIC;\n", type, newname);

    /*
     * We still need some hacking though to cover the case where new default
     * public privileges are added in new versions: the REVOKE ALL will revoke
     * them, leading to behavior different from what the old version had,
     * which is generally not what's wanted.  So add back default privs if the
     * source database is too old to have had that particular priv.
     */
    if (remoteVersion < 80200 && strcmp(type, "DATABASE") == 0) {
        /* database CONNECT priv didn't exist before 8.2 */
        (void)appendPQExpBuffer(firstsql, "%sGRANT CONNECT ON %s %s TO PUBLIC;\n", prefix, type, newname);
    }

    /* Scan individual ACL items */
    for (i = 0; i < naclitems; i++) {
        /* must use the base value */
        if (!parseAclItem(aclitems[i], type, name, subname, remoteVersion, grantee, grantor, privs, privswgo)) {
            (void)destroyPQExpBuffer(grantee);
            (void)destroyPQExpBuffer(grantor);
            (void)destroyPQExpBuffer(privs);
            (void)destroyPQExpBuffer(privswgo);
            (void)destroyPQExpBuffer(firstsql);
            (void)destroyPQExpBuffer(secondsql);
            free(aclitems);
            aclitems = NULL;
            free(newname);
            newname = NULL;
            return false;
        }

        if (grantor->len == 0 && (owner != NULL))
            (void)printfPQExpBuffer(grantor, "%s", owner);

        if (privs->len > 0 || privswgo->len > 0) {
            if ((owner != NULL) && strcmp(grantee->data, owner) == 0 && strcmp(grantor->data, owner) == 0) {
                found_owner_privs = true;

                /*
                 * For the owner, the default privilege level is ALL WITH
                 * GRANT OPTION (only ALL prior to 7.4).
                 */
                if (supports_grant_options(remoteVersion) ? strcmp(privswgo->data, "ALL") != 0
                                                          : strcmp(privs->data, "ALL") != 0) {
                    char* temp_grantee_username = grantee->data;

                    if ((binary_upgrade_oldowner != NULL) &&
                        (0 == strncmp(temp_grantee_username, binary_upgrade_oldowner, NAMEDATALEN))) {
                        temp_grantee_username = binary_upgrade_newowner;
                    }

                    if (!hasRevoke) {
                        (void)appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
                        if (NULL != subname)
                            (void)appendPQExpBuffer(firstsql, "(%s)", subname);
                        (void)appendPQExpBuffer(
                            firstsql, " ON %s %s FROM %s;\n", type, newname, fmtId(temp_grantee_username));
                        hasRevoke = true;
                    }
                    if (privs->len > 0)
                        (void)appendPQExpBuffer(firstsql,
                            "%sGRANT %s ON %s %s TO %s;\n",
                            prefix,
                            privs->data,
                            type,
                            newname,
                            fmtId(temp_grantee_username));
                    if (privswgo->len > 0)
                        (void)appendPQExpBuffer(firstsql,
                            "%sGRANT %s ON %s %s TO %s WITH GRANT OPTION;\n",
                            prefix,
                            privswgo->data,
                            type,
                            newname,
                            fmtId(temp_grantee_username));
                }
            } else {
                /*
                 * Otherwise can assume we are starting from no privs.
                 */
                if (grantor->len > 0 && ((owner == NULL) || strcmp(owner, grantor->data) != 0)) {
                    if ((binary_upgrade_oldowner != NULL) &&
                        (0 == strncmp(grantor->data, binary_upgrade_oldowner, NAMEDATALEN))) {
                        rolepassword = getRolePassword(conn, binary_upgrade_newowner);
                        (void)appendPQExpBuffer(secondsql, "SET SESSION AUTHORIZATION '%s' ", binary_upgrade_newowner);
                        (void)appendPQExpBuffer(secondsql, "PASSWORD %s;\n", rolepassword);
                    } else {
                        rolepassword = getRolePassword(conn, grantor->data);
                        (void)appendPQExpBuffer(secondsql, "SET SESSION AUTHORIZATION '%s' ", grantor->data);
                        (void)appendPQExpBuffer(secondsql, "PASSWORD %s;\n", rolepassword);
                    }
                    rc = memset_s(rolepassword, strlen(rolepassword), 0, strlen(rolepassword));
                    securec_check_c(rc, "\0", "\0");
                    free(rolepassword);
                    rolepassword = NULL;
                }

                if (privs->len > 0) {
                    (void)appendPQExpBuffer(secondsql, "%sGRANT %s ON %s %s TO ", prefix, privs->data, type, newname);
                    if (grantee->len == 0)
                        (void)appendPQExpBuffer(secondsql, "PUBLIC;\n");
                    else if (strncmp(grantee->data, "group ", strlen("group ")) == 0)
                        (void)appendPQExpBuffer(secondsql, "GROUP %s;\n", fmtId(grantee->data + strlen("group ")));
                    else {
                        if ((binary_upgrade_oldowner != NULL) &&
                            (0 == strncmp(grantor->data, binary_upgrade_oldowner, NAMEDATALEN))) {
                            (void)appendPQExpBuffer(secondsql, "%s;\n", fmtId(binary_upgrade_newowner));
                        } else {
                            (void)appendPQExpBuffer(secondsql, "%s;\n", fmtId(grantee->data));
                        }
                    }
                }
                if (privswgo->len > 0) {
                    (void)appendPQExpBuffer(
                        secondsql, "%sGRANT %s ON %s %s TO ", prefix, privswgo->data, type, newname);
                    if (grantee->len == 0)
                        (void)appendPQExpBuffer(secondsql, "PUBLIC");
                    else if (strncmp(grantee->data, "group ", strlen("group ")) == 0)
                        (void)appendPQExpBuffer(secondsql, "GROUP %s", fmtId(grantee->data + strlen("group ")));
                    else {
                        if ((binary_upgrade_oldowner != NULL) &&
                            (0 == strncmp(grantor->data, binary_upgrade_oldowner, NAMEDATALEN))) {
                            (void)appendPQExpBuffer(secondsql, "%s", fmtId(binary_upgrade_newowner));
                        } else {
                            (void)appendPQExpBuffer(secondsql, "%s", fmtId(grantee->data));
                        }
                    }
                    (void)appendPQExpBuffer(secondsql, " WITH GRANT OPTION;\n");
                }

                if (grantor->len > 0 && ((owner == NULL) || strcmp(owner, grantor->data) != 0))
                    (void)appendPQExpBuffer(secondsql, "RESET SESSION AUTHORIZATION;\n");
            }
        }
    }

    /*
     * If we didn't find any owner privs, the owner must have revoked 'em all
     */
    if (!found_owner_privs && (NULL != owner)) {
        (void)appendPQExpBuffer(firstsql, "%sREVOKE ALL", prefix);
        if (NULL != subname)
            (void)appendPQExpBuffer(firstsql, "(%s)", subname);

        if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(owner, binary_upgrade_oldowner, NAMEDATALEN))) {
            owner = binary_upgrade_newowner;
        }
        (void)appendPQExpBuffer(firstsql, " ON %s %s FROM %s;\n", type, newname, fmtId(owner));
    }

    (void)destroyPQExpBuffer(grantee);
    (void)destroyPQExpBuffer(grantor);
    (void)destroyPQExpBuffer(privs);
    (void)destroyPQExpBuffer(privswgo);

    (void)appendPQExpBuffer(sql, "%s%s", firstsql->data, secondsql->data);
    (void)destroyPQExpBuffer(firstsql);
    (void)destroyPQExpBuffer(secondsql);

    free(aclitems);
    aclitems = NULL;
    free(newname);
    newname = NULL;
    return true;
}

/*
 * Build ALTER DEFAULT PRIVILEGES command(s) for single pg_default_acl entry.
 *
 *	type: the object type (TABLES, FUNCTIONS, etc)
 *	nspname: schema name, or NULL for global default privileges
 *	acls: the ACL string fetched from the database
 *	owner: username of privileges owner (will be passed through fmtId)
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 */
bool buildDefaultACLCommands(const char* type, const char* nspname, const char* acls, const char* owner,
    int remoteVersion, PQExpBuffer sql, PGconn* conn)
{
    bool result = false;
    PQExpBuffer prefix;

    prefix = createPQExpBuffer();

    /*
     * We incorporate the target role directly into the command, rather than
     * playing around with SET ROLE or anything like that.	This is so that a
     * permissions error leads to nothing happening, rather than changing
     * default privileges for the wrong user.
     */
    (void)appendPQExpBuffer(prefix, "ALTER DEFAULT PRIVILEGES FOR ROLE %s ", fmtId(owner));
    if (nspname != NULL)
        (void)appendPQExpBuffer(prefix, "IN SCHEMA %s ", fmtId(nspname));

    result = buildACLCommands("", NULL, type, acls, owner, prefix->data, remoteVersion, sql, conn);

    (void)destroyPQExpBuffer(prefix);

    return result;
}

/*
 * This will parse an aclitem string, having the general form
 *		username=privilegecodes/grantor
 * or
 *		group groupname=privilegecodes/grantor
 * (the /grantor part will not be present if pre-7.4 database).
 *
 * The returned grantee string will be the dequoted username or groupname
 * (preceded with "group " in the latter case).  The returned grantor is
 * the dequoted grantor name or empty.	Privilege characters are decoded
 * and split between privileges with grant option (privswgo) and without
 * (privs).
 *
 * Note: for cross-version compatibility, it's important to use ALL when
 * appropriate.
 */
static bool parseAclItem(const char* item, const char* type, const char* name, const char* subname, int remoteVersion,
    PQExpBuffer grantee, PQExpBuffer grantor, PQExpBuffer privs, PQExpBuffer privswgo)
{
    char* buf = NULL;
    bool all_with_go = true;
    bool all_without_go = true;
    char* eqpos = NULL;
    char* slpos = NULL;
    char* pos = NULL;

    buf = strdup(item);
    if (buf == NULL)
        return false;

    /* user or group name is string up to = */
    eqpos = copyAclUserName(grantee, buf);
    if (*eqpos != '=') {
        free(buf);
        buf = NULL;
        return false;
    }

    /* grantor may be listed after / */
    slpos = strchr(eqpos + 1, '/');
    if (slpos != NULL) {
        *slpos++ = '\0';
        slpos = copyAclUserName(grantor, slpos);
        if (*slpos != '\0') {
            free(buf);
            buf = NULL;
            return false;
        }
    } else
        (void)resetPQExpBuffer(grantor);

        /* privilege codes */
#define CONVERT_PRIV(code, keywd)                 \
    do {                                          \
        if ((pos = strchr(eqpos + 1, code))) {    \
            if (*(pos + 1) == '*') {              \
                AddAcl(privswgo, keywd, subname); \
                all_without_go = false;           \
            } else {                              \
                AddAcl(privs, keywd, subname);    \
                all_with_go = false;              \
            }                                     \
        } else                                    \
            all_with_go = all_without_go = false; \
    } while (0)

    (void)resetPQExpBuffer(privs);
    (void)resetPQExpBuffer(privswgo);

    if (strcmp(type, "TABLE") == 0 || strcmp(type, "SEQUENCE") == 0 || strcmp(type, "TABLES") == 0 ||
        strcmp(type, "SEQUENCES") == 0 || strcmp(type, "LARGE SEQUENCE") == 0) {
        CONVERT_PRIV('r', "SELECT");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('m', "COMMENT");
        }

        if (strcmp(type, "SEQUENCE") == 0 || strcmp(type, "SEQUENCES") == 0 || strcmp(type, "LARGE SEQUENCE") == 0) {
            /* sequence only */
            CONVERT_PRIV('U', "USAGE");

            if (remoteVersion >= 90200) {
                CONVERT_PRIV('A', "ALTER");
                CONVERT_PRIV('P', "DROP");
            }
        } else {
            /* table only */
            CONVERT_PRIV('a', "INSERT");
            if (remoteVersion >= 70200) {
                CONVERT_PRIV('x', "REFERENCES");
            }
            /* rest are not applicable to columns */
            if (subname == NULL) {
                if (remoteVersion >= 70200) {
                    CONVERT_PRIV('d', "DELETE");
                    CONVERT_PRIV('t', "TRIGGER");
                }
                if (remoteVersion >= 80400) {
                    CONVERT_PRIV('D', "TRUNCATE");
                }
                if (remoteVersion >= 90200) {
                    CONVERT_PRIV('A', "ALTER");
                    CONVERT_PRIV('P', "DROP");
                    CONVERT_PRIV('i', "INDEX");
                    CONVERT_PRIV('v', "VACUUM");
                }
            }
        }

        /* UPDATE */
        if (remoteVersion >= 70200 || strcmp(type, "SEQUENCE") == 0 || strcmp(type, "SEQUENCES") == 0 ||
            strcmp(type, "LARGE SEQUENCE") == 0)
            CONVERT_PRIV('w', "UPDATE");
        else
            /* 7.0 and 7.1 have a simpler worldview */
            CONVERT_PRIV('w', "UPDATE,DELETE");
    } else if (strcmp(type, "FUNCTION") == 0 
                   || strcmp(type, "FUNCTIONS") == 0
                   || strcmp(type, "PROCEDURE") == 0 
                   || strcmp(type, "PROCEDURES") == 0) {
        CONVERT_PRIV('X', "EXECUTE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "LANGUAGE") == 0) {
        CONVERT_PRIV('U', "USAGE");
    } else if (strcmp(type, "SCHEMA") == 0) {
        CONVERT_PRIV('C', "CREATE");
        CONVERT_PRIV('U', "USAGE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "DATABASE") == 0) {
        CONVERT_PRIV('C', "CREATE");
        CONVERT_PRIV('c', "CONNECT");
        CONVERT_PRIV('T', "TEMPORARY");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "TABLESPACE") == 0) {
        CONVERT_PRIV('C', "CREATE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "TYPE") == 0 || strcmp(type, "TYPES") == 0) {
        CONVERT_PRIV('U', "USAGE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "FOREIGN DATA WRAPPER") == 0) {
        CONVERT_PRIV('U', "USAGE");
    } else if (strcmp(type, "FOREIGN SERVER") == 0) {
        CONVERT_PRIV('U', "USAGE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
        }
    } else if (strcmp(type, "FOREIGN TABLE") == 0) {
        CONVERT_PRIV('r', "SELECT");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
            CONVERT_PRIV('m', "COMMENT");
            CONVERT_PRIV('i', "INDEX");
            CONVERT_PRIV('v', "VACUUM");
        }
    } else if (strcmp(type, "LARGE OBJECT") == 0) {
        CONVERT_PRIV('r', "SELECT");
        CONVERT_PRIV('w', "UPDATE");
    } else if (strcmp(type, "NODE GROUP") == 0) {
        CONVERT_PRIV('C', "CREATE");
        CONVERT_PRIV('U', "USAGE");
        CONVERT_PRIV('p', "COMPUTE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
        }
    } else if (strcmp(type, "DATA SOURCE") == 0) {
        CONVERT_PRIV('U', "USAGE");
    } else if (strcmp(type, "DIRECTORY") == 0) {
        CONVERT_PRIV('R', "READ");
        CONVERT_PRIV('W', "WRITE");
        if (remoteVersion >= 90200) {
            CONVERT_PRIV('A', "ALTER");
            CONVERT_PRIV('P', "DROP");
        }
    } else if (strcmp(type, "GLOBAL SETTINGS") == 0 || strcmp(type, "COLUMN SETTINGS") == 0) {
        CONVERT_PRIV('U', "USAGE");
        CONVERT_PRIV('P', "DROP");
    } else if (strcmp(type, "PACKAGE") == 0 || strcmp(type, "PACKAGE BODY") == 0) {
        CONVERT_PRIV('X', "EXECUTE");
        CONVERT_PRIV('A', "ALTER");
        CONVERT_PRIV('P', "DROP");
        CONVERT_PRIV('m', "COMMENT");
    } else {
        abort();
    }

#undef CONVERT_PRIV

    if (all_with_go) {
        (void)resetPQExpBuffer(privs);
        (void)printfPQExpBuffer(privswgo, "ALL");
        if (NULL != subname)
            (void)appendPQExpBuffer(privswgo, "(%s)", subname);
    } else if (all_without_go) {
        (void)resetPQExpBuffer(privswgo);
        (void)printfPQExpBuffer(privs, "ALL");
        if (NULL != subname)
            (void)appendPQExpBuffer(privs, "(%s)", subname);
    }

    free(buf);
    buf = NULL;

    return true;
}

/*
 * Transfer a user or group name starting at *input into the output buffer,
 * dequoting if needed.  Returns a pointer to just past the input name.
 * The name is taken to end at an unquoted '=' or end of string.
 */
static char* copyAclUserName(PQExpBuffer output, const char* input)
{
    (void)resetPQExpBuffer(output);

    while (*input && *input != '=') {
        /*
         * If user name isn't quoted, then just add it to the output buffer
         */
        if (*input != '"') {
            (void)appendPQExpBufferChar(output, *input++);
        } else {
            /* Otherwise, it's a quoted username */
            input++;
            /* Loop until we come across an unescaped quote */
            while (!(*input == '"' && *(input + 1) != '"')) {
                if (*input == '\0') {
                    return (char*)input; /* really a syntax error... */
                }

                /*
                 * Quoting convention is to escape " as "".  Keep this code in
                 * sync with putid() in backend's acl.c.
                 */
                if (*input == '"' && *(input + 1) == '"') {
                    input++;
                }
                (void)appendPQExpBufferChar(output, *input++);
            }
            input++;
        }
    }
    return (char*)input;
}

/*
 * Append a privilege keyword to a keyword list, inserting comma if needed.
 */
static void AddAcl(PQExpBuffer aclbuf, const char* keyword, const char* subname)
{
    if (aclbuf->len > 0)
        (void)appendPQExpBufferChar(aclbuf, ',');
    (void)appendPQExpBuffer(aclbuf, "%s", keyword);
    if (subname != NULL)
        (void)appendPQExpBuffer(aclbuf, "(%s)", subname);
}

/*
 * processSQLNamePattern
 *
 * Scan a wildcard-pattern string and generate appropriate WHERE clauses
 * to limit the set of objects returned.  The WHERE clauses are appended
 * to the already-partially-constructed query in buf.  Returns whether
 * any clause was added.
 *
 * conn: connection query will be sent to (consulted for escaping rules).
 * buf: output parameter.
 * pattern: user-specified pattern option, or NULL if none ("*" is implied).
 * have_where: true if caller already emitted "WHERE" (clauses will be ANDed
 * onto the existing WHERE clause).
 * force_escape: always quote regexp special characters, even outside
 * double quotes (else they are quoted only between double quotes).
 * schemavar: name of query variable to match against a schema-name pattern.
 * Can be NULL if no schema.
 * namevar: name of query variable to match against an object-name pattern.
 * altnamevar: NULL, or name of an alternative variable to match against name.
 * visibilityrule: clause to use if we want to restrict to visible objects
 * (for example, "pg_catalog.pg_table_is_visible(p.oid)").	Can be NULL.
 *
 * Formatting note: the text already present in buf should end with a newline.
 * The appended text, if any, will end with one too.
 */
bool processSQLNamePattern(PGconn* conn, PQExpBuffer buf, const char* pattern, bool have_where, bool force_escape,
    const char* schemavar, const char* namevar, const char* altnamevar, const char* visibilityrule)
{
    PQExpBufferData schemabuf;
    PQExpBufferData namebuf;
    int encoding = PQclientEncoding(conn);
    bool inquotes = false;
    const char* cp = NULL;
    int i = 0;
    bool added_clause = false;

#define WHEREAND() (appendPQExpBufferStr(buf, have_where ? "  AND " : "WHERE "), have_where = true, added_clause = true)

    if (pattern == NULL) {
        /* Default: select all visible objects */
        if (NULL != visibilityrule) {
            WHEREAND();
            appendPQExpBuffer(buf, "%s\n", visibilityrule);
        }
        return added_clause;
    }

    initPQExpBuffer(&schemabuf);
    initPQExpBuffer(&namebuf);

    /*
     * Parse the pattern, converting quotes and lower-casing unquoted letters.
     * Also, adjust shell-style wildcard characters into regexp notation.
     *
     * We surround the pattern with "^(...)$" to force it to match the whole
     * string, as per SQL practice.  We have to have parens in case the string
     * contains "|", else the "^" and "$" will be bound into the first and
     * last alternatives which is not what we want.
     *
     * Note: the result of this pass is the actual regexp pattern(s) we want
     * to execute.	Quoting/escaping into SQL literal format will be done
     * below using appendStringLiteralConn().
     */
    appendPQExpBufferStr(&namebuf, "^(");

    inquotes = false;
    cp = pattern;

    while (*cp) {
        char ch = *cp;

        if (ch == '"') {
            if (inquotes && cp[1] == '"') {
                /* emit one quote, stay in inquotes mode */
                appendPQExpBufferChar(&namebuf, '"');
                cp++;
            } else {
                inquotes = !inquotes;
            }
            cp++;
        } else if (!inquotes && isupper((unsigned char)ch)) {
            appendPQExpBufferChar(&namebuf, pg_tolower((unsigned char)ch));
            cp++;
        } else if (!inquotes && ch == '*') {
            appendPQExpBufferStr(&namebuf, ".*");
            cp++;
        } else if (!inquotes && ch == '?') {
            appendPQExpBufferChar(&namebuf, '.');
            cp++;
        } else if (!inquotes && ch == '.') {
            /* Found schema/name separator, move current pattern to schema */
            resetPQExpBuffer(&schemabuf);
            appendPQExpBufferStr(&schemabuf, namebuf.data);
            resetPQExpBuffer(&namebuf);
            appendPQExpBufferStr(&namebuf, "^(");
            cp++;
        } else if (ch == '$') {
            /*
             * Dollar is always quoted, whether inside quotes or not. The
             * reason is that it's allowed in SQL identifiers, so there's a
             * significant use-case for treating it literally, while because
             * we anchor the pattern automatically there is no use-case for
             * having it possess its regexp meaning.
             */
            appendPQExpBufferStr(&namebuf, "\\$");
            cp++;
        } else {
            /*
             * Ordinary data character, transfer to pattern
             *
             * Inside double quotes, or at all times if force_escape is true,
             * quote regexp special characters with a backslash to avoid
             * regexp errors.  Outside quotes, however, let them pass through
             * as-is; this lets knowledgeable users build regexp expressions
             * that are more powerful than shell-style patterns.
             */
            if ((inquotes || force_escape) && (strchr("|*+?()[]{}.^$\\", ch) != NULL))
                appendPQExpBufferChar(&namebuf, '\\');
            i = PQmblen(cp, encoding);
            while (i-- && *cp) {
                appendPQExpBufferChar(&namebuf, *cp);
                cp++;
            }
        }
    }

    /*
     * Now decide what we need to emit.  Note there will be a leading "^(" in
     * the patterns in any case.
     */
    if (namebuf.len > 2) {
        /* We have a name pattern, so constrain the namevar(s) */
        appendPQExpBufferStr(&namebuf, ")$");
        /* Optimize away a "*" pattern */
        if (strcmp(namebuf.data, "^(.*)$") != 0) {
            WHEREAND();
            if (altnamevar != NULL) {
                appendPQExpBuffer(buf, "(%s ~ ", namevar);
                appendStringLiteralConn(buf, namebuf.data, conn);
                appendPQExpBuffer(buf, "\n        OR %s ~ ", altnamevar);
                appendStringLiteralConn(buf, namebuf.data, conn);
                appendPQExpBufferStr(buf, ")\n");
            } else {
                appendPQExpBuffer(buf, "%s ~ ", namevar);
                appendStringLiteralConn(buf, namebuf.data, conn);
                appendPQExpBufferChar(buf, '\n');
            }
        }
    }

    if (schemabuf.len > 2) {
        /* We have a schema pattern, so constrain the schemavar */
        appendPQExpBufferStr(&schemabuf, ")$");
        /* Optimize away a "*" pattern */
        if (strcmp(schemabuf.data, "^(.*)$") != 0 && (schemavar != NULL)) {
            WHEREAND();
            appendPQExpBuffer(buf, "%s ~ ", schemavar);
            appendStringLiteralConn(buf, schemabuf.data, conn);
            appendPQExpBufferChar(buf, '\n');
        }
    } else {
        /* No schema pattern given, so select only visible objects */
        if (visibilityrule != NULL) {
            WHEREAND();
            appendPQExpBuffer(buf, "%s\n", visibilityrule);
        }
    }

    termPQExpBuffer(&schemabuf);
    termPQExpBuffer(&namebuf);

    return added_clause;
#undef WHEREAND
}

/*
 * buildShSecLabelQuery
 *
 * Build a query to retrieve security labels for a shared object.
 */
void buildShSecLabelQuery(PGconn* conn, const char* catalog_name, uint32 objectId, PQExpBuffer sql)
{
    (void)appendPQExpBuffer(sql,
        "SELECT provider, label FROM pg_catalog.pg_shseclabel "
        "WHERE classoid = '%s'::pg_catalog.regclass AND "
        "objoid = %u",
        catalog_name,
        objectId);
}

/*
 * emitShSecLabels
 *
 * Format security label data retrieved by the query generated in
 * buildShSecLabelQuery.
 */
void emitShSecLabels(PGconn* conn, PGresult* res, PQExpBuffer buffer, const char* target, const char* objname)
{
    int i;

    for (i = 0; i < PQntuples(res); i++) {
        char* provider = PQgetvalue(res, i, 0);
        char* label = PQgetvalue(res, i, 1);

        /* must use fmtId result before calling it again */
        appendPQExpBuffer(buffer, "SECURITY LABEL FOR %s ON %s", fmtId(provider), target);
        appendPQExpBuffer(buffer, " %s IS ", fmtId(objname));
        appendStringLiteralConn(buffer, label, conn);
        appendPQExpBuffer(buffer, ";\n");
    }
}

/*
 * Parse a --section=foo command line argument.
 *
 * Set or update the bitmask in *dumpSections according to arg.
 * dumpSections is initialised as DUMP_UNSECTIONED by pg_dump and
 * pg_restore so they can know if this has even been called.
 */
void set_dump_section(const char* arg, int* dumpSections)
{
    /* if this is the first call, clear all the bits */
    if (*dumpSections == DUMP_UNSECTIONED)
        *dumpSections = 0;

    uint32 dumpSectionsTemp = *dumpSections;
    if (strcmp(arg, "pre-data") == 0)
        dumpSectionsTemp |= DUMP_PRE_DATA;
    else if (strcmp(arg, "data") == 0)
        dumpSectionsTemp |= DUMP_DATA;
    else if (strcmp(arg, "post-data") == 0)
        dumpSectionsTemp |= DUMP_POST_DATA;
    else {
        write_stderr(_("%s: unrecognized section name: \"%s\"\n"), progname, arg);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        exit_nicely(1);
    }
    *dumpSections = dumpSectionsTemp;
}

/*
 * Write a printf-style message to stderr.
 *
 * The program name is prepended, if "progname" has been set.
 * Also, if modulename isn't NULL, that's included too.
 * Note that we'll try to translate the modulename and the fmt string.
 */
void write_msg(const char* modulename, const char* fmt, ...)
{
    va_list ap;

    (void)va_start(ap, fmt);
    vwrite_msg(modulename, fmt, ap);
    va_end(ap);
}

/*
 * As write_msg, but pass a va_list not variable arguments.
 */
void vwrite_msg(const char* modulename, const char* fmt, va_list ap)
{
#define MAX_ERRBUF_SIZE 2048
    char errbuf[MAX_ERRBUF_SIZE] = {0};
    int len = 0;
    int nRet = 0;

    time_t current_time;
    struct tm* system = NULL;
    char current_localtime[30] = {0};

    current_time = time(NULL);
    system = localtime(&current_time);
    if (NULL != system) {
        strftime(current_localtime, sizeof(current_localtime), "%Y-%m-%d %H:%M:%S", system);
    }

    if ((NULL != progname) && (NULL != instport)) {
        if (modulename != NULL) {
            if (dbname != NULL) {
                nRet = snprintf_s(errbuf,
                    MAX_ERRBUF_SIZE,
                    sizeof(errbuf) - 1,
                    "%s: [port='%s'] [%s] [%s] [%s] ",
                    progname,
                    instport,
                    dbname,
                    _(modulename),
                    current_localtime);
            } else {
                nRet = snprintf_s(errbuf,
                    MAX_ERRBUF_SIZE,
                    sizeof(errbuf) - 1,
                    "%s: [port='%s'] [%s] [%s] ",
                    progname,
                    instport,
                    _(modulename),
                    current_localtime);
            }
        } else {
            if (dbname != NULL) {
                nRet = snprintf_s(errbuf,
                    MAX_ERRBUF_SIZE,
                    sizeof(errbuf) - 1,
                    "%s[port='%s'][%s][%s]: ",
                    progname,
                    instport,
                    dbname,
                    current_localtime);
            } else {
                nRet = snprintf_s(errbuf,
                    MAX_ERRBUF_SIZE,
                    sizeof(errbuf) - 1,
                    "%s[port='%s'][%s]: ",
                    progname,
                    instport,
                    current_localtime);
            }
        }
        securec_check_ss_c(nRet, "\0", "\0");
        len += strlen(errbuf);
    }

    // This place just is the message print. So there is't need check the value of vsnprintf_s function return. if
    // checked, when the message lengtn is over than MAX_ERRBUF_SIZE, will be abnormal exit.
    (void)vsnprintf_s(errbuf + len, sizeof(errbuf) - (len), sizeof(errbuf) - (len)-1, fmt, ap);
    if (strlen(errbuf) >= 2044) {
        errbuf[MAX_ERRBUF_SIZE - 1] = '\0';
        errbuf[MAX_ERRBUF_SIZE - 2] = '\n';
        errbuf[MAX_ERRBUF_SIZE - 3] = '.';
        errbuf[MAX_ERRBUF_SIZE - 4] = '.';
        errbuf[MAX_ERRBUF_SIZE - 5] = '.';
    }

    write_stderr("%s", errbuf);
}

/*
 * Fail and die, with a message to stderr.	Parameters as for write_msg.
 */
void exit_horribly(const char* modulename, const char* fmt, ...)
{
    va_list ap;

    (void)va_start(ap, fmt);
    vwrite_msg(modulename, fmt, ap);
    va_end(ap);

    exit_nicely(1);
}

/* Register a callback to be run when exit_nicely is invoked. */
void on_exit_nicely(on_exit_nicely_callback function, void* arg)
{
    if (on_exit_nicely_index >= MAX_ON_EXIT_NICELY)
        exit_horribly(NULL, "out of on_exit_nicely slots\n");
    on_exit_nicely_list[on_exit_nicely_index].function = function;
    on_exit_nicely_list[on_exit_nicely_index].arg = arg;
    on_exit_nicely_index++;
}

/*
 * Run accumulated on_exit_nicely callbacks in reverse order and then exit
 * quietly.  This needs to be thread-safe.
 */
void exit_nicely(int code)
{
    int i;

    for (i = on_exit_nicely_index - 1; i >= 0; i--)
        (*on_exit_nicely_list[i].function)(code, on_exit_nicely_list[i].arg);

    exit(code);
}

void simple_string_list_append(SimpleStringList* list, const char* val)
{
    SimpleStringListCell* cell = NULL;

    /* this calculation correctly accounts for the null trailing byte */
    cell = (SimpleStringListCell*)pg_malloc(sizeof(SimpleStringListCell) + strlen(val));
    cell->next = NULL;

    errno_t rc = strncpy_s(cell->val, (strlen(val) + 1), val, strlen(val));
    securec_check_c(rc, "\0", "\0");
    (cell->val)[strlen(val)] = '\0';

    if (NULL != (list->tail))
        list->tail->next = cell;
    else
        list->head = cell;
    list->tail = cell;
}

bool simple_string_list_member(SimpleStringList* list, const char* val)
{
    SimpleStringListCell* cell = NULL;

    for (cell = list->head; cell != NULL; cell = cell->next) {
        /* Use strcmp to prevent the occurrence of subsets */
        if (strcmp(cell->val, val) == 0)
            return true;
    }

    return false;
}

bool simple_oid_list_member(SimpleOidList* list, Oid val)
{
    SimpleOidListCell* cell = NULL;

    for (cell = list->head; cell != NULL; cell = cell->next) {
        if (cell->val == val)
            return true;
    }
    return false;
}

/*
 * Support for simple list operations
 */
void simple_oid_list_append(SimpleOidList* list, Oid val)
{
    SimpleOidListCell* cell = NULL;

    cell = (SimpleOidListCell*)pg_malloc(sizeof(SimpleOidListCell));
    cell->next = NULL;
    cell->val = val;

    if (NULL != (list->tail))
        list->tail->next = cell;
    else
        list->head = cell;
    list->tail = cell;
}

/*
 * Replace the actual password with *'s.
 */
void replace_password(int argc, char** argv, const char* optionName)
{
    int count = 0;
    char* pchPass = NULL;
    char* pchTemp = NULL;

    // Check if password option is specified in command line
    for (count = 0; count < argc; count++) {
        // Password can be specified by optionName
        if (strncmp(optionName, argv[count], strlen(optionName)) == 0) {
            pchTemp = strchr(argv[count], '=');
            if (pchTemp != NULL) {
                pchPass = pchTemp + 1;
            } else if ((NULL != strstr(argv[count], optionName)) && (strlen(argv[count]) > strlen(optionName))) {
                pchPass = argv[count] + strlen(optionName);
            } else {
                pchPass = argv[(int)(count + 1)];
            }

            // Replace first char of password with * and rest clear it
            if (strlen(pchPass) > 0) {
                *pchPass = '*';
                pchPass = pchPass + 1;
                while ('\0' != *pchPass) {
                    *pchPass = '\0';
                    pchPass++;
                }
            }

            break;
        }
    }
}

/*
 * Lock of the catalog with pg_rman.ini file and return 0.
 * If the lock is held by another one, return 1 immediately.
 */
int catalog_lock(const char* filePath)
{
    int ret;

    lock_fd = open(filePath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (lock_fd == -1) {
        exit_horribly(NULL, "could not open output file \"%s\": %s\n", filePath, strerror(errno));
    }

    ret = dump_flock(lock_fd, LOCK_EX | LOCK_NB); /* non-blocking */
    if (ret == -1) {
        if (errno == EWOULDBLOCK) {
            (void)close(lock_fd);
            lock_fd = -1;

            // Somebody has locked, may be another process is trying to dump or restore using the same file
            exit_horribly(NULL, "Another process is trying to dump/restore using the same file\n");
        } else {
            (void)close(lock_fd);
            lock_fd = -1;
            exit_horribly(NULL, "Acquiring lock on file \"%s\" failed: %s\n", filePath, strerror(errno));
        }
    }

    return 0;
}

/*
 * Release catalog lock.
 */
void catalog_unlock(int code, void* arg)
{
    if (-1 != lock_fd) {
        (void)close(lock_fd);
        lock_fd = -1;
    }
}

void remove_lock_file(int code, void* arg)
{
    (void)remove((const char*)arg);
}

int dump_flock(int fd, uint32 operation)
{
#ifndef WIN32_PG_DUMP
    struct flock lck;
    int cmd;

    errno_t rc = memset_s(&lck, sizeof(lck), 0, sizeof(lck));
    securec_check_c(rc, "\0", "\0");
    lck.l_whence = SEEK_SET;
    lck.l_start = 0;
    lck.l_len = 0;
    lck.l_pid = getpid();

    if (operation & LOCK_UN)
        lck.l_type = F_UNLCK;
    else if (operation & LOCK_EX)
        lck.l_type = F_WRLCK;
    else
        lck.l_type = F_RDLCK;

    if (operation & LOCK_NB)
        cmd = F_SETLK;
    else
        cmd = F_SETLKW;

    return fcntl(fd, cmd, &lck);
#else
    return pgut_flock(fd, operation, 0, 0, 0);
#endif
}

#ifdef WIN32_PG_DUMP
/*
 * GetEnvStr
 *
 * Note: malloc space for get the return of getenv() function, then return the malloc space.
 *         so, this space need be free.
 */
static char* GetEnvStr(const char* env)
{
    char* tmpvar = NULL;
    const char* temp = getenv(env);
    errno_t rc = 0;
    if (temp != NULL) {
        size_t len = strlen(temp);
        if (0 >= len)
            return NULL;
        tmpvar = (char*)malloc(len + 1);
        if (tmpvar != NULL) {
            rc = strcpy_s(tmpvar, len + 1, temp);
            securec_check_c(rc, "\0", "\0");
            return tmpvar;
        }
    }
    return NULL;
}
#endif
/*
 * Return true if the path is a existing regular file.
 */
bool fileExists(const char* path)
{
    struct stat buf;

    if (stat(path, &buf) == -1 && errno == ENOENT) {
        return false;
    } else if (!S_ISREG(buf.st_mode)) {
        return false;
    } else {
        return true;
    }
}

bool IsDir(const char* dirpath)
{
    struct stat st;
    return stat(dirpath, &st) == 0 && S_ISDIR(st.st_mode);
}

char* make_absolute_path(const char* path)
{
    char* result = NULL;
    char* cwdbuf = NULL;
    size_t len = 0;
    int size = 0;
    int nRet = 0;

    if (is_absolute_path(path)) {
        result = strdup(path);
        if (result == NULL)
            exit_horribly(NULL, "out of memory\n");
    } else {

#ifndef WIN32_PG_DUMP
        size = pathconf(".", _PC_PATH_MAX);
#else
        {
            char* tmp = GetEnvStr("cd");
            size = -1;
            if (tmp != NULL) {
                size = strlen(tmp);
                if (size >= 1024) {
                    size = -1;
                }
                pwd = NULL;
                free(tmp);
                tmp = NULL;
            }
        }
#endif
        if (-1 == size) {
            exit_horribly(NULL, "could not get maximum length of current working directory: %s\n", strerror(errno));
        }
        cwdbuf = (char*)pg_malloc((size_t)size);
        if (getcwd(cwdbuf, size) == NULL) {
            exit_horribly(NULL, "could not get current working directory: %s\n", strerror(errno));
        }

        /* This 2 stand for '/' and '\0' */
        len = size + strlen(path) + 2;
        result = (char*)pg_malloc(len);

        nRet = snprintf_s(result, len, len - 1, "%s/%s", cwdbuf, path);
        securec_check_ss_c(nRet, result, "\0");
        free(cwdbuf);
        cwdbuf = NULL;
    }

    canonicalize_path(result);
    return result;
}

/*
 * @Description:whether the SQL commands has result
 */
bool isExistsSQLResult(PGconn* conn, const char* sqlCmd)
{
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int ntups;

    printfPQExpBuffer(buf, "%s;", sqlCmd);
    res = PQexec(conn, buf->data);
    ntups = PQntuples(res);

    PQclear(res);
    destroyPQExpBuffer(buf);

    if (ntups > 0) {
        return true;
    } else {
        return false;
    }
}
/*
 * @Description: check if the column name is in the system class
 * @in relid : the oid of system class from pg_catalog.
       column_name : the column name in the system class.
 */
bool is_column_exists(PGconn* conn, Oid relid, const char* column_name)
{
    PQExpBuffer query = createPQExpBuffer();
    bool isExists = false;

    printfPQExpBuffer(query,
        "SELECT true AS b_rolvalidbegin "
        "FROM pg_catalog.pg_attribute "
        "WHERE attrelid = %u AND attname = '%s';",
        relid,
        column_name);
    isExists = isExistsSQLResult(conn, query->data);
    destroyPQExpBuffer(query);

    return isExists;
}
