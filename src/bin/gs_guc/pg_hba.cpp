/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *---------------------------------------------------------------------------------------
 *
 *  pg_hba.cpp
 *        the interface for user to set the hba configuration
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/pg_hba.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#ifdef WIN32
/*
 * Need this to get defines for restricted tokens and jobs. And it
 * has to be set before any header from the Win32 API is loaded.
 */
#define _WIN32_WINNT 0x0501
#endif

#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#include <sys/file.h>
#include <fcntl.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"
#include "flock.h"
#include "libpq/hba.h"
#include "libpq/pqsignal.h"
#include "getopt_long.h"
#include "miscadmin.h"
#include "common/config/cm_config.h"
#include "securec.h"
#include "securec_check.h"

#if defined(__CYGWIN__)
#include <sys/cygwin.h>
#include <windows.h>
/* Cygwin defines WIN32 in windows.h, but we don't want it. */
#undef WIN32
#endif
char* HbaFileName;
const int MAX_PARAM_LEN = 1024;
const int MAX_VALUE_LEN = 1024;
#define INVALID_LINES_IDX (int)(~0)
const int EQUAL_MARK_LEN = 3;
#define MAX_CONFIG_ITEM 1024
#define PG_LOCKFILE_SIZE 1024
#define ADDPOINTER_SIZE 2
#define MAXLISTEN 64

#define SUCCESS 0
#define FAILURE 1

extern char pid_file[MAXPGPATH];
extern char gucconf_file[MAXPGPATH];
extern char tempguc_file[MAXPGPATH];
extern char gucconf_lock_file[MAXPGPATH];

#define atooid(x) ((Oid)strtoul((x), NULL, 10))
#define atoxid(x) ((TransactionId)strtoul((x), NULL, 10))
#define token_is_keyword(t, k) (!(t)->quoted && pg_strcasecmp((t)->string, (k)) == 0)
#define token_matches(t, k) (pg_strcasecmp((t)->string, (k)) == 0)

#define MAX_TOKEN 256

#define GS_FREE(ptr)         \
    do {                     \
        if (NULL != (ptr)) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)

/* callback data for check_network_callback */
typedef struct check_network_data {
    IPCompareMethod method; /* test method */
    SockAddr* raddr;        /* client's actual address */
    bool result;            /* set to true if match */
} check_network_data;

char *g_hbaType = NULL;
char *g_hbaDatabase = NULL;
char *g_hbaUser = NULL;
char *g_hbaAddr = NULL;

/*
 * A single string token lexed from the HBA config file, together with whether
 * the token had been quoted.
 */
typedef struct HbaToken {
    char* string;
    bool quoted;
} HbaToken;

/*
 * @@GaussDB@@
 * Brief           :update or add config_parameter
 * Description :
 */
typedef enum { UPDATE_PARAMETER = 0, ADD_PARAMETER } UpdateOrAddParameter;

typedef struct HbaSortLine {
    char* line;
    int line_num;
    bool commented;
    bool valid_hbaline;
    HbaLine new_hbaline;
} HbaSortLine;

typedef enum {
    CODE_OK = 0,                 // success
    CODE_UNKNOW_CONFFILE_PATH,   // can not find config file at the path
    CODE_OPEN_CONFFILE_FAILED,   // failed to open config file
    CODE_CLOSE_CONFFILE_FAILED,  // failed to close config file
    CODE_READE_CONFFILE_ERROR,   // failed to read config file
    CODE_WRITE_CONFFILE_ERROR,   // failed to write config file
    CODE_LOCK_CONFFILE_FAILED,
    CODE_UNLOCK_CONFFILE_FAILED,
    CODE_UNKOWN_ERROR
} ErrCode;

typedef struct {
    FILE* fp;
    size_t size;
} FileLock;

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
#define INVALID_AUTH_OPTION(optname, validmethods)                                                                    \
    do {                                                                                                              \
        if (!search_in_comment) {                                                                                     \
            if (line_num != 0) {                                                                                      \
                write_stderr("WARRNING: authentication option \"%s\" is only valid for authentication methods %s at " \
                             "line number %d\n",                                                                      \
                    optname,                                                                                          \
                    validmethods,                                                                                     \
                    line_num);                                                                                        \
            } else {                                                                                                  \
                write_stderr("ERROR: Invalid argument as authentication option \"%s\" is only valid for "             \
                             "authentication methods %s.\n",                                                          \
                    optname,                                                                                          \
                    validmethods);                                                                                    \
            }                                                                                                         \
        }                                                                                                             \
        return false;                                                                                                 \
    } while (0);

#define REQUIRE_AUTH_OPTION(methodval, optname, validmethods) \
    do {                                                      \
        if (hbaline->auth_method != (methodval))              \
            INVALID_AUTH_OPTION(optname, validmethods);       \
    } while (0);

#define MANDATORY_AUTH_ARG(argvar, argname, authname)                                                                 \
    do {                                                                                                              \
        if ((argvar) == NULL) {                                                                                       \
            if (!search_in_comment) {                                                                                 \
                if (line_num != 0) {                                                                                  \
                    write_stderr(                                                                                     \
                        "WARRNING: authentication method \"%s\" requires argument \"%s\" to be set line number %d\n", \
                        authname,                                                                                     \
                        argname,                                                                                      \
                        line_num);                                                                                    \
                } else {                                                                                              \
                    write_stderr("ERROR: Invalid argument as authentication method \"%s\" requires argument \"%s\" "  \
                                 "to be set\n",                                                                       \
                        authname,                                                                                     \
                        argname);                                                                                     \
                }                                                                                                     \
            }                                                                                                         \
            return NULL;                                                                                              \
        }                                                                                                             \
    } while (0);

#define INVALID_HBA_LINE0(emsg, search_comment, lineno)                           \
    do {                                                                          \
        if (!(search_comment)) {                                                  \
            if ((lineno) == 0) {                                                  \
                write_stderr("ERROR: Invalid argument as " emsg "\n");            \
            } else {                                                              \
                write_stderr("WARNNING: " emsg " in line number: %d.\n", lineno); \
            }                                                                     \
        }                                                                         \
    } while (0)

#define INVALID_HBA_LINE1(emsg, arg1, search_comment, lineno)                           \
    do {                                                                                \
        if (!(search_comment)) {                                                        \
            if ((lineno) == 0) {                                                        \
                write_stderr("ERROR: Invalid argument as " emsg "\n", arg1);            \
            } else {                                                                    \
                write_stderr("WARNNING: " emsg " in line number: %d.\n", arg1, lineno); \
            }                                                                           \
        }                                                                               \
    } while (0)

#define INVALID_HBA_LINE2(emsg, a1, a2, search_comment, lineno)                           \
    do {                                                                                  \
        if (!(search_comment)) {                                                          \
            if ((lineno) == 0) {                                                          \
                write_stderr("ERROR: Invalid argument  as " emsg "\n", a1, a2);           \
            } else {                                                                      \
                write_stderr("WARNNING: " emsg " in line number: %d.\n", a1, a2, lineno); \
            }                                                                             \
        }                                                                                 \
    } while (0)

void do_config_reload();
void trimBlanksTwoEnds(char** strPtr);

extern char** config_param;
extern char** hba_param;
extern char** config_value;
extern int config_param_number;
extern int config_value_number;
extern int arraysize;
extern const char* progname;
static HbaLine* parse_hba_line(const char* line, int line_num, HbaLine* hbaline, bool search_in_comment);
static HbaLine* parse_hba_line_options(
    const char* line, int line_num, HbaLine* parsedline, bool search_in_comment, int* pcur_line_pos);

char* xstrdup(const char* s);
void* pg_malloc(size_t size);
void* pg_malloc_zero(size_t size);
bool isOptLineCommented(const char* optLine);
void do_advice(void);
ErrCode get_file_lock(const char* path, FileLock* filelock);
void release_file_lock(FileLock* filelock);
ErrCode writefile(char* path, char** lines, UpdateOrAddParameter isAddorUpdate);
void free_space(char** optlines, int size);
static bool parse_hba_auth_opt(char* name, const char* val, HbaLine* hbaline, int line_num, bool search_in_comment);

extern void do_hba_analysis(const char* strcmd);
extern int do_hba_set(const char *action_type);
HbaSortLine* sort_hba_lines(const char** optlines, int reserve_num_lines);
void insert_into_sorted_hbalines(HbaSortLine* sortedHbalines, int inpos);
static void free_hba_line(HbaLine* parsedline);
extern void free_hba_params();
extern int check_config_file_status();
extern char** backup_config_file(const char* read_file, char* write_file, FileLock filelock, int reserve_num_lines);
extern int do_parameter_value_write(char** opt_lines, UpdateOrAddParameter updateoradd);
static bool verify_ip(const char* compare_ip);

void write_stderr(const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

extern "C" {
void freefile(char** lines);
char** readfile(const char* path, int reserve_num_lines);
}

bool pg_isblank(const char c)
{
    return c == ' ' || c == '\t' || c == '\r';
}

static List* new_list(NodeTag type)
{
    List* new_list_val = NULL;
    ListCell* new_head = NULL;

    new_head = (ListCell*)pg_malloc(sizeof(*new_head));
    new_head->next = NULL;
    /* new_head->data is left undefined! */

    new_list_val = (List*)pg_malloc(sizeof(*new_list_val));
    new_list_val->type = type;
    new_list_val->length = 1;
    new_list_val->head = new_head;
    new_list_val->tail = new_head;

    return new_list_val;
}

static void new_tail_cell(List* list)
{
    ListCell* new_tail = NULL;

    new_tail = (ListCell*)pg_malloc(sizeof(*new_tail));
    new_tail->next = NULL;

    list->tail->next = new_tail;
    list->tail = new_tail;
    list->length++;
}

List* lappend(List* list, void* datum)
{
    if (list == NIL)
        list = new_list(T_List);
    else
        new_tail_cell(list);

    lfirst(list->tail) = datum;
    return list;
}

static bool match_sockaddr_AF_INET(const struct sockaddr_in* addr1, const struct sockaddr_in* addr2,
    const struct sockaddr_in* addr1mask, const struct sockaddr_in* addr2mask)
{

    if ((addr1mask->sin_addr.s_addr == addr2mask->sin_addr.s_addr) &&
        ((addr1->sin_addr.s_addr & addr1mask->sin_addr.s_addr) ==
            (addr2->sin_addr.s_addr & addr2mask->sin_addr.s_addr))) {
        return true;
    }

    return false;
}

#ifdef HAVE_IPV6

static bool match_sockaddr_AF_INET6(struct sockaddr_in6* addr1, struct sockaddr_in6* addr2,
    struct sockaddr_in6* addr1mask, struct sockaddr_in6* addr2mask)
{
    int i;
    for (i = 0; i < 16; i++) {
        if (addr1mask->sin6_addr.s6_addr[i] != addr2mask->sin6_addr.s6_addr[i]) {
            return false;
        }

        if ((addr1->sin6_addr.s6_addr[i] & addr1mask->sin6_addr.s6_addr[i]) !=
            (addr2->sin6_addr.s6_addr[i] & addr2mask->sin6_addr.s6_addr[i])) {
            return false;
        }
    }

    return true;
}
#endif /* HAVE_IPV6 */

/*
 * pg_range_sockaddr - is addr within the subnet specified by netaddr/netmask ?
 *
 * Note: caller must already have verified that all three addresses are
 * in the same address family; and AF_UNIX addresses are not supported.
 */
bool match_sockaddr(struct sockaddr_storage* addr1, struct sockaddr_storage* addr2, struct sockaddr_storage* netmask1,
    struct sockaddr_storage* netmask2)
{
    if (addr1->ss_family == AF_INET) {
       return match_sockaddr_AF_INET((struct sockaddr_in*)addr1,
            (struct sockaddr_in*)addr2,
            (struct sockaddr_in*)netmask1,
            (struct sockaddr_in*)netmask2); 
    }
        
#ifdef HAVE_IPV6
    else if (addr1->ss_family == AF_INET6)
        return match_sockaddr_AF_INET6((struct sockaddr_in6*)addr1,
            (struct sockaddr_in6*)addr2,
            (struct sockaddr_in6*)netmask1,
            (struct sockaddr_in6*)netmask2);
#endif
    else
        return false;
}

/*
 * Check to see if a connecting IP matches the given address and netmask.
 */
static bool match_ip(struct sockaddr* addr1, struct sockaddr* addr2, struct sockaddr* mask1, struct sockaddr* mask2)
{
    if (addr1->sa_family == addr2->sa_family) {
        /* Same address family */
        if (!match_sockaddr((struct sockaddr_storage*)addr1,
                (struct sockaddr_storage*)addr2,
                (struct sockaddr_storage*)mask1,
                (struct sockaddr_storage*)mask2))
            return false;
    } else {
        /* Wrong address family, no IPV6 */
        return false;
    }

    return true;
}

/*
 *	pg_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 */
int pg_getaddrinfo_all(
    const char* hostname, const char* servname, const struct addrinfo* hintp, struct addrinfo** result)
{
    int rc;

    /* not all versions of getaddrinfo() zero *result on failure */
    *result = NULL;

    /* NULL has special meaning to getaddrinfo(). */
    rc = getaddrinfo(((hostname == NULL) || hostname[0] == '\0') ? NULL : hostname, servname, hintp, result);

    return rc;
}

/*
 *	pg_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix
 *
 * Note: the ai_family field of the original hint structure must be passed
 * so that we can tell whether the addrinfo struct was built by the system's
 * getaddrinfo() routine or our own getaddrinfo_unix() routine.  Some versions
 * of getaddrinfo() might be willing to return AF_UNIX addresses, so it's
 * not safe to look at ai_family in the addrinfo itself.
 */
void pg_freeaddrinfo_all(struct addrinfo* ai)
{
    /* struct was built by getaddrinfo() */
    if (ai != NULL)
        freeaddrinfo(ai);
}

int pg_sockaddr_cidr_mask(struct sockaddr_storage* mask, const char* numbits, int family)
{
    long bits;
    char* endptr = NULL;
    errno_t rc;

    bits = strtol(numbits, &endptr, 10);

    if (*numbits == '\0' || *endptr != '\0') {
        return -1;
    }        

    switch (family) {
        case AF_INET: {
            struct sockaddr_in mask4;
            long maskl;

            if (bits < 0 || bits > 32) {
                return -1;
            }
                
            rc = memset_s(&mask4, sizeof(mask4), 0, sizeof(mask4));
            securec_check_c(rc, "\0", "\0");
            /* avoid "x << 32", which is not portable */
            if (bits > 0) {
                maskl = (0xffffffffUL << (32 - (int)bits)) & 0xffffffffUL;
            }                
            else {
                maskl = 0;
            }
                
            mask4.sin_addr.s_addr = htonl(maskl);
            rc = memcpy_s(mask, sizeof(struct sockaddr_storage), &mask4, sizeof(mask4));
            securec_check_c(rc, "\0", "\0");
            break;
        }

#ifdef HAVE_IPV6
        case AF_INET6: {
            struct sockaddr_in6 mask6;
            int i;

            if (bits < 0 || bits > 128)
                return -1;
            rc = memset_s(&mask6, sizeof(mask6), 0, sizeof(mask6));
            securec_check_c(rc, "\0", "\0");
            for (i = 0; i < 16; i++) {
                if (bits <= 0)
                    mask6.sin6_addr.s6_addr[i] = 0;
                else if (bits >= 8)
                    mask6.sin6_addr.s6_addr[i] = 0xff;
                else {
                    mask6.sin6_addr.s6_addr[i] = (0xff << (8 - (int)bits)) & 0xff;
                }
                bits -= 8;
            }
            rc = memcpy_s(mask, sizeof(struct sockaddr_storage), &mask6, sizeof(mask6));
            securec_check_c(rc, "\0", "\0");
            break;
        }
#endif
        default:
            return -1;
    }

    mask->ss_family = family;
    return 0;
}

int cmp_ip_type_and_prefix(struct sockaddr* mask1, struct sockaddr* mask2)
{
    if (mask1->sa_family != mask2->sa_family) {
        if (AF_INET == mask1->sa_family) {
            return 1;
        }
        return -1;
    }

    if (AF_INET == mask1->sa_family) {
        struct sockaddr_in* ipv4mask1 = (struct sockaddr_in*)mask1;
        struct sockaddr_in* ipv4mask2 = (struct sockaddr_in*)mask2;

        if (((uint32)ipv4mask1->sin_addr.s_addr) > ((uint32)ipv4mask2->sin_addr.s_addr)) {
            return 1;
        } else if (((uint32)ipv4mask1->sin_addr.s_addr) < ((uint32)ipv4mask2->sin_addr.s_addr)) {
            return -1;
        } else
            return 0;
    }

#ifdef HAVE_IPV6
    else if (mask1->sa_family == AF_INET6) {
        struct sockaddr_in6* ipv6mask1 = (struct sockaddr_in6*)mask1;
        struct sockaddr_in6* ipv6mask2 = (struct sockaddr_in6*)mask2;
        int i;

        for (i = 0; i < 16; i++) {
            if (ipv6mask1->sin6_addr.s6_addr[i] != ipv6mask2->sin6_addr.s6_addr[i]) {
                return (ipv6mask1->sin6_addr.s6_addr[i] - ipv6mask2->sin6_addr.s6_addr[i]);
            }
        }
        return 0;
    }
#endif
    else
        return 1;
}

static bool match_list(List* tokens_new, List* tokens_file)
{
    ListCell* cell_new = NULL;
    HbaToken* tok_new = NULL;
    ListCell* cell_file = NULL;
    HbaToken* tok_file = NULL;

    if (tokens_new->length != tokens_file->length) {
        return false;
    }

    forboth(cell_new, tokens_new, cell_file, tokens_file)
    {
        tok_new = (HbaToken*)lfirst(cell_new);
        tok_file = (HbaToken*)lfirst(cell_file);
        if ((tok_new->quoted != tok_file->quoted) ||
            (0 != strncmp(tok_new->string, tok_file->string, strlen(tok_new->string) + 1))) {
            return false;
        }
    }

    return true;
}

bool exact_match_hba_line(HbaLine* new_hbaline, HbaLine* file_habaline)
{
    if ((new_hbaline->conntype != file_habaline->conntype) ||
        ((ctLocal != new_hbaline->conntype) && (new_hbaline->ip_cmp_method != file_habaline->ip_cmp_method))) {
        return false;
    }

    /* Check IP address */
    if ((ctLocal != new_hbaline->conntype) && (ipCmpMask == new_hbaline->ip_cmp_method)) {
        if ((((new_hbaline->hostname) != NULL) && (file_habaline->hostname == NULL)) ||
            ((new_hbaline->hostname == NULL) && ((file_habaline->hostname) != NULL))) {
            return false;
        }

        if ((new_hbaline->hostname) != NULL) {
            if (0 != strncmp(new_hbaline->hostname, file_habaline->hostname, strlen(new_hbaline->hostname) + 1)) {
                return false;
            }
        } else {
            if (!match_ip((struct sockaddr*)&new_hbaline->addr,
                    (struct sockaddr*)&file_habaline->addr,
                    (struct sockaddr*)&new_hbaline->mask,
                    (struct sockaddr*)&file_habaline->mask)) {
                return false;
            }
        }
    }

    /* Check database and role */
    if ((!match_list(new_hbaline->databases, file_habaline->databases)) ||
        (!match_list(new_hbaline->roles, file_habaline->roles))) {
        return false;
    }

    return true;
}

bool process_hba_line(char** optlines, HbaLine* new_hbaline, const char* arghbaline)
{
    bool isMatched = false;
    HbaSortLine* sorted_hbalines = NULL;
    int i = 0;
    int cur_line_pos = 0;
    char* newline = NULL;
    int slen = 0;
    int comment_len = 0;
    int nRet = 0;
    errno_t rc = 0;

    if (NULL == optlines) {
        return (INVALID_LINES_IDX != 0);
    }

    sorted_hbalines = sort_hba_lines((const char**)optlines, 3);

    for (i = 0; sorted_hbalines[i].line != NULL; i++) {
        if (!sorted_hbalines[i].commented && sorted_hbalines[i].valid_hbaline) {
            isMatched = exact_match_hba_line(new_hbaline, &sorted_hbalines[i].new_hbaline);
            if (isMatched) {
                break;
            }
        }
    }

    if (!isMatched) {
        for (i = 0; sorted_hbalines[i].line != NULL; i++) {
            if (sorted_hbalines[i].commented && sorted_hbalines[i].valid_hbaline) {
                isMatched = exact_match_hba_line(new_hbaline, &sorted_hbalines[i].new_hbaline);
                if (isMatched) {
                    break;
                }
            }
        }
    }

    /* Found and update is required */
    if (isMatched) {
        if (new_hbaline->auth_method == uaImplicitReject) {
            /* Deletion case and already commeted, so no modification required */
            if (sorted_hbalines[i].commented) {
                for (i = 0; sorted_hbalines[i].line != NULL; i++) {
                    free_hba_line(&sorted_hbalines[i].new_hbaline);
                }
                GS_FREE(sorted_hbalines);
                return false;
            }
            slen = strlen(sorted_hbalines[i].line);
            newline = (char*)pg_malloc(slen + 4);
            nRet = snprintf_s(newline, (slen + 4), slen + 3, "#%s", sorted_hbalines[i].line);
            securec_check_ss_c(nRet, newline, "\0");
            free(sorted_hbalines[i].line);
            sorted_hbalines[i].line = newline;
        } else {
            /* preserve comment part into updated line */
            /* skip intial comments */
            cur_line_pos = 0;
            while (
                (pg_isblank(sorted_hbalines[i].line[cur_line_pos])) || (sorted_hbalines[i].line[cur_line_pos] == '#'))
                cur_line_pos++;
            /* scan till find the '#' which is not in '"' */
            while ((sorted_hbalines[i].line[cur_line_pos] != '\n') && (sorted_hbalines[i].line[cur_line_pos] != '\0') &&
                   (sorted_hbalines[i].line[cur_line_pos] != '#'))
                cur_line_pos++;

            comment_len = strlen(sorted_hbalines[i].line + cur_line_pos);
            slen = strlen(arghbaline);
            newline = (char*)pg_malloc(slen + 4 + comment_len);
            comment_len = snprintf_s(newline,
                (slen + 4 + comment_len),
                (slen + 3 + comment_len),
                "%s %s",
                arghbaline,
                (sorted_hbalines[i].line + cur_line_pos));
            securec_check_ss_c(comment_len, newline, "\0");
            if (newline[comment_len - 1] != '\n') {
                newline[comment_len] = '\n';
                newline[(long)comment_len + 1] = '\0';
            }

            free(sorted_hbalines[i].line);
            sorted_hbalines[i].line = newline;
        }
    } else {
        /* HBA Line not found and deletion case, so no modification required */
        if (new_hbaline->auth_method == uaImplicitReject) {
            for (i = 0; sorted_hbalines[i].line != NULL; i++) {
                free_hba_line(&sorted_hbalines[i].new_hbaline);
            }
            GS_FREE(sorted_hbalines);
            return false;
        }

        /* insert new HBA into sorted list */
        slen = strlen(arghbaline);
        newline = (char*)pg_malloc(slen + 3);
        rc = strncpy_s(newline, (slen + 3), arghbaline, slen + 1);
        securec_check_c(rc, newline, "\0");
        newline[slen] = '\n';
        newline[(long)slen + 1] = '\0';
        sorted_hbalines[i].line = newline;
        if (NULL == parse_hba_line(newline, i + 1, &sorted_hbalines[i].new_hbaline, false)) {
            for (i = 0; sorted_hbalines[i].line != NULL; i++) {
                free_hba_line(&sorted_hbalines[i].new_hbaline);
            }
            GS_FREE(sorted_hbalines);
            return false;
        }
        sorted_hbalines[i].valid_hbaline = true;

        insert_into_sorted_hbalines(sorted_hbalines, i);
    }

    for (i = 0; sorted_hbalines[i].line != NULL; i++) {
        optlines[i] = sorted_hbalines[i].line;
        free_hba_line(&sorted_hbalines[i].new_hbaline);
    }

    GS_FREE(sorted_hbalines);

    return true;
}

HbaSortLine* sort_hba_lines(const char** optlines, int reserve_num_lines)
{
    int i = 0;
    HbaSortLine* sortedHbalines = NULL;

    if (NULL == optlines) {
        return (HbaSortLine*)pg_malloc_zero(sizeof(HbaSortLine) * (1 + reserve_num_lines));
    }

    for (i = 0; optlines[i] != NULL; i++)
        ;

    sortedHbalines = (HbaSortLine*)pg_malloc_zero(sizeof(HbaSortLine) * (i + 1 + reserve_num_lines));

    /* BEGIN for gs_guc */
    /* The first loop is to deal with the lines not commented by '#' */
    for (i = 0; optlines[i] != NULL; i++) {
        sortedHbalines[i].line = (char*)optlines[i];
        sortedHbalines[i].line_num = i + 1;

        if (!isOptLineCommented(optlines[i])) {
            sortedHbalines[i].commented = false;
            if (NULL == parse_hba_line(optlines[i], i + 1, &sortedHbalines[i].new_hbaline, false)) {
                sortedHbalines[i].valid_hbaline = false;
                continue;
            }
            sortedHbalines[i].valid_hbaline = true;
        } else {
            sortedHbalines[i].commented = true;
            if (NULL == parse_hba_line(optlines[i], i + 1, &sortedHbalines[i].new_hbaline, true)) {
                sortedHbalines[i].valid_hbaline = false;
                continue;
            }
            sortedHbalines[i].valid_hbaline = true;
        }

        insert_into_sorted_hbalines(sortedHbalines, i);
    }

    return sortedHbalines;
}

bool list_has_keyword(List* tokens_new, const char* keyword)
{
    ListCell* cell_new = NULL;
    HbaToken* tok_new = NULL;
    bool bkeyword_found = false;

    foreach (cell_new, tokens_new) {
        tok_new = (HbaToken*)lfirst(cell_new);
        if (!tok_new->quoted && (0 == strncmp(tok_new->string, keyword, strlen(tok_new->string) + 1))) {
            bkeyword_found = true;
        }
    }

    return bkeyword_found;
}

int cmp_lists(List* list1, List* list2)
{
    bool list1_has_all = false;
    bool list2_has_all = false;

    list1_has_all = list_has_keyword(list1, "all");

    list2_has_all = list_has_keyword(list2, "all");

    if (list1_has_all == list2_has_all) {
        return list2->length - list1->length;
    }

    /* If HBA Line use "all", let it in the back */
    if (list1_has_all) {
        return -1;
    }

    return 1;
}

int cmp_hba_line(HbaLine* hbaline1, HbaLine* hbaline2)
{
    int connection_type_order[] = {7, 4, 6, 5, 3, 0, 2, 1}; /* order: local > hostssl > hostnossl > host */
    /* ipCmpMask, ipCmpSameHost, ipCmpSameNet, ipCmpAll */
    int ipaddr_order[] = {1, 3, 2, 0};
    bool list1_has_replication = false;
    bool list2_has_replication = false;
    int retval = 0;

    /* compare replication database */
    list1_has_replication = (hbaline1->databases->length == 1) && list_has_keyword(hbaline1->databases, "replication");
    list2_has_replication = (hbaline2->databases->length == 1) && list_has_keyword(hbaline2->databases, "replication");
    if (list1_has_replication != list2_has_replication) {
        return ((connection_type_order[hbaline1->conntype + (4 * (int)list1_has_replication)]) -
                (connection_type_order[hbaline2->conntype + (4 * (int)list2_has_replication)]));
    }

    /* compare hostnames/ipaddress */
    if ((ctLocal != hbaline1->conntype) && (hbaline1->ip_cmp_method != hbaline2->ip_cmp_method)) {
        return (ipaddr_order[hbaline1->ip_cmp_method] - ipaddr_order[hbaline2->ip_cmp_method]);
    }

    if ((ctLocal != hbaline1->conntype) && (ipCmpMask == hbaline1->ip_cmp_method)) {
        if ((NULL != hbaline1->hostname) || (NULL != hbaline2->hostname)) {
            return (((int)(NULL != hbaline1->hostname)) - ((int)(NULL != hbaline2->hostname)));
        }

        /* compare ipaddress [ipv4/ipv6] */
        retval = cmp_ip_type_and_prefix((struct sockaddr*)&hbaline1->mask, (struct sockaddr*)&hbaline2->mask);
        if (retval != 0) {
            return retval;
        }
    }

    /* compare connection type */
    if (hbaline1->conntype != hbaline2->conntype) {
        return (connection_type_order[hbaline1->conntype] - connection_type_order[hbaline2->conntype]);
    }

    /* compare database names */
    retval = cmp_lists(hbaline1->databases, hbaline2->databases);
    if (retval != 0) {
        return retval;
    }

    /* compare usernames names */
    retval = cmp_lists(hbaline1->roles, hbaline2->roles);
    if (retval != 0) {
        return retval;
    }

    return 0;
}

void insert_into_sorted_hbalines(HbaSortLine* sortedHbalines, int inpos)
{
    int idx = inpos - 1;
    int min_valid_line = inpos;
    int insertpos;
    int retval;
    HbaSortLine temp{};

    while (idx >= 0) {
        if (sortedHbalines[idx].valid_hbaline) {
            retval = cmp_hba_line(&sortedHbalines[idx].new_hbaline, &sortedHbalines[inpos].new_hbaline);
            if (retval >= 0) {
                break;
            }
            min_valid_line = idx;
        }

        idx--;
    }

    /* No change is required */
    if (min_valid_line == inpos) {
        return;
    }

    /* to skip the comments which are in begining of file */
    if (idx == -1) {
        insertpos = min_valid_line;
    } else {
        insertpos = idx + 1;
    }

    temp.line = sortedHbalines[inpos].line;
    temp.line_num = sortedHbalines[inpos].line_num;
    temp.commented = sortedHbalines[inpos].commented;
    temp.valid_hbaline = sortedHbalines[inpos].valid_hbaline;
    temp.new_hbaline = sortedHbalines[inpos].new_hbaline;

    /* move till idx */
    for (idx = inpos; idx > insertpos; idx--) {
        sortedHbalines[idx].line = sortedHbalines[idx - 1].line;
        sortedHbalines[idx].line_num = sortedHbalines[idx - 1].line_num;
        sortedHbalines[idx].commented = sortedHbalines[idx - 1].commented;
        sortedHbalines[idx].valid_hbaline = sortedHbalines[idx - 1].valid_hbaline;
        sortedHbalines[idx].new_hbaline = sortedHbalines[idx - 1].new_hbaline;
    }

    sortedHbalines[idx].line = temp.line;
    sortedHbalines[idx].line_num = temp.line_num;
    sortedHbalines[idx].commented = temp.commented;
    sortedHbalines[idx].valid_hbaline = temp.valid_hbaline;
    sortedHbalines[idx].new_hbaline = temp.new_hbaline;

    return;
}

static HbaToken* make_hba_token(const char* token, bool quoted)
{
    HbaToken* hbatoken = NULL;
    int toklen;
    errno_t rc = 0;

    toklen = strlen(token);
    hbatoken = (HbaToken*)pg_malloc(sizeof(HbaToken) + toklen + 1);
    hbatoken->string = (char*)hbatoken + sizeof(HbaToken);
    hbatoken->quoted = quoted;
    rc = memcpy_s(hbatoken->string, toklen + 1, token, toklen + 1);
    securec_check_c(rc, "\0", "\0");

    return hbatoken;
}

static void free_tokens(List* tokens)
{
    ListCell* cell = NULL;
    ListCell* cell_next = NULL;
    HbaToken* token = NULL;
    if (NIL == tokens) {
        return;
    }

    cell = list_head(tokens);
    while (cell != NULL) {
        cell_next = lnext(cell);
        token = (HbaToken*)lfirst(cell);
        GS_FREE(token);
        free(cell);
        cell = cell_next;
    }

    GS_FREE(tokens);
}

static bool next_token(
    const char* line, int* line_cur_pos, char* buf, int bufsz, bool* initial_quote, bool* terminating_comma)
{
    int c;
    char* start_buf = buf;
    /* end_buf reserves two bytes to ensure we can append \n and \0 */
    char* end_buf = buf + (bufsz - 2);
    bool in_quote = false;
    bool was_quote = false;
    bool saw_quote = false;

    *initial_quote = false;
    *terminating_comma = false;

    /* Move over initial whitespace and commas */
    while ((c = line[(*line_cur_pos)++]) != '\0' && (pg_isblank(c) || c == ','))
        ;

    if (c == '\0' || c == '\n') {
        *buf = '\0';
        return false;
    }

    /*
     * Build a token in buf of next characters up to EOF, EOL, unquoted comma,
     * or unquoted whitespace.
     */
    while (c != '\0' && c != '\n' && (!pg_isblank(c) || in_quote)) {
        /* skip comments to EOL */
        if (c == '#' && !in_quote) {
            while ((c = line[(*line_cur_pos)++]) != '\0' && c != '\n')
                ;
            break;
        }

        if (buf >= end_buf) {
            *buf = '\0';
            write_stderr("WARNNING: token too long, skipping: \"%s\"", start_buf);
            /* Discard remainder of line */
            while ((c = line[(*line_cur_pos)++]) != '\0' && c != '\n')
                ;
            break;
        }

        /* we do not pass back the comma in the token */
        if (c == ',' && !in_quote) {
            *terminating_comma = true;
            break;
        }

        if (c != '"' || was_quote) {
            *buf++ = c;
        }            

        /* Literal double-quote is two double-quotes */
        if (in_quote && c == '"') {
            was_quote = !was_quote;
        } else {
            was_quote = false;
        }

        if (c == '"') {
            in_quote = !in_quote;
            saw_quote = true;
            if (buf == start_buf) {
                *initial_quote = true;
            }
        }

        c = line[(*line_cur_pos)++];
    }

    /*
     * Put back the char right after the token (critical in case it is EOL,
     * since we need to detect end-of-line at next call).
     */
    (*line_cur_pos)--;

    *buf = '\0';

    return (saw_quote || buf > start_buf);
}

static List* next_field_expand(const char* line, int* line_cur_pos)
{
    char buf[MAX_TOKEN] = {0};
    bool trailing_comma = false;
    bool initial_quote = false;
    List* tokens = NIL;

    do {
        if (!next_token(line, line_cur_pos, buf, sizeof(buf), &initial_quote, &trailing_comma)) {
            break;
        }

        /* Is this referencing a file? */
        tokens = lappend(tokens, make_hba_token(buf, initial_quote));
    } while (trailing_comma);

    return tokens;
}

static void free_hba_line(HbaLine* parsedline)
{
    errno_t rc = 0;
    if (NULL == parsedline) {
        return;
    }

    free_tokens(parsedline->databases);
    parsedline->databases = NULL;
    free_tokens(parsedline->roles);
    parsedline->roles = NULL;
    GS_FREE(parsedline->hostname);
    GS_FREE(parsedline->usermap);
    GS_FREE(parsedline->pamservice);
    GS_FREE(parsedline->ldapserver);
    GS_FREE(parsedline->ldapbinddn);
    GS_FREE(parsedline->ldapbindpasswd);
    GS_FREE(parsedline->ldapsearchattribute);
    GS_FREE(parsedline->ldapbasedn);
    GS_FREE(parsedline->ldapprefix);
    GS_FREE(parsedline->ldapsuffix);
    GS_FREE(parsedline->krb_server_hostname);
    GS_FREE(parsedline->krb_realm);

    rc = memset_s(parsedline, sizeof(HbaLine), '\0', sizeof(HbaLine));
    securec_check_c(rc, "\0", "\0");

    return;
}

void free_hba_params()
{
    int i;
    for (i = 0; i < config_param_number; i++) {
        free_hba_line((HbaLine*)config_param[i]);
    }
}

static bool verify_ip(const char* compare_ip)
{
    uint32 nodeidx = 0;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        /* coordinator */
        if (g_node[nodeidx].coordinate == 1) {
            uint32 kk = 0;
            for (kk = 0; kk < g_node[nodeidx].coordinateListenCount; kk++) {
                if ((compare_ip != NULL) && 0 == strncmp(g_node[nodeidx].coordinateListenIP[kk], compare_ip, MAXLISTEN))
                    return true;
            }
        }
        /* datanode */
        if (g_node[nodeidx].datanodeCount != 0) {
            uint32 kk = 0;
            for (kk = 0; kk < g_node[nodeidx].datanodeCount; kk++) {
                uint32 tt = 0;
                for (tt = 0; tt < g_node[nodeidx].datanode[kk].datanodeListenCount; tt++) {
                    if ((compare_ip != NULL) &&
                        0 == strncmp(g_node[nodeidx].datanode[kk].datanodeListenIP[tt], compare_ip, MAXLISTEN))
                        return true;
                }
            }
        }
    }

    return false;
}

static HbaLine* parse_hba_line(const char* line, int line_num, HbaLine* parsedline, bool search_in_comment)
{
    struct addrinfo* gai_result = NULL;
    struct addrinfo hints {};
    int ret = 0;
    char* cidr_slash = NULL;
    char* unsupauth = NULL;
    char* conntip = NULL;
    List* tokens = NULL;
    HbaToken* token = NULL;
    char tmp_str[MAXLISTEN] = {0};
    bool isconntype = false;
    bool isdatabase = false;
    bool isroles = false;
    bool isip = false;
    parsedline->linenumber = line_num;
    int cur_line_pos = 0;

    if (search_in_comment) {
        while ((pg_isblank(line[cur_line_pos])) || (line[cur_line_pos] == '#'))
            cur_line_pos++;
    }

    /* Check the record type. */
    tokens = next_field_expand(line, &cur_line_pos);
    if (NIL == tokens) {
        return NULL;
    }

    if (tokens->length > 1) {

        INVALID_HBA_LINE0("multiple values specified for connection type", search_in_comment, line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    token = (HbaToken*)linitial(tokens);
    if (strncmp(token->string, "local", sizeof("local")) == 0) {
#ifdef HAVE_UNIX_SOCKETS
        parsedline->conntype = ctLocal;
#else
        INVALID_HBA_LINE0("local connections are not supported by this build", search_in_comment, line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
#endif
    } else if (strncmp(token->string, "host", sizeof("host")) == 0 ||
               strncmp(token->string, "hostssl", sizeof("hostssl")) == 0 ||
               strncmp(token->string, "hostnossl", sizeof("hostnossl")) == 0) {
        if (strncmp(token->string, "host", sizeof("host")) == 0) /* "host" */
        {
            isconntype = true;
        }

        if (token->string[4] == 's') /* "hostssl" */
        {
            /* SSL support must be actually active, else complain */
#ifdef USE_SSL
            parsedline->conntype = ctHostSSL;
#else
            INVALID_HBA_LINE0("hostssl is not supported by this build", search_in_comment, line_num);
            free_tokens(tokens);
            tokens = NULL;
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
        INVALID_HBA_LINE1("invalid connection type \"%s\" ", token->string, search_in_comment, line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    GS_FREE(g_hbaType);
    g_hbaType = xstrdup(token->string);

    free_tokens(tokens);
    tokens = NULL;

    tokens = next_field_expand(line, &cur_line_pos);
    if (NIL == tokens) {
        INVALID_HBA_LINE0("end-of-line before database specification ", search_in_comment, line_num);
        return NULL;
    }
    parsedline->databases = tokens;

    token = (HbaToken*)linitial(tokens);
    if (strncmp(token->string, "all", sizeof("all")) == 0) {
        isdatabase = true;
    }

    GS_FREE(g_hbaDatabase);
    g_hbaDatabase = xstrdup(token->string);

    tokens = next_field_expand(line, &cur_line_pos);
    if (NIL == tokens) {
        INVALID_HBA_LINE0("end-of-line before role specification ", search_in_comment, line_num);
        return NULL;
    }
    parsedline->roles = tokens;
    token = (HbaToken*)linitial(tokens);
    if (strncmp(token->string, "all", sizeof("all")) == 0) {
        isroles = true;
    }

    GS_FREE(g_hbaUser);
    g_hbaUser = xstrdup(token->string);

    if (parsedline->conntype != ctLocal) {
        /* Read the IP address field. (with or without CIDR netmask) */
        tokens = next_field_expand(line, &cur_line_pos);
        if (NIL == tokens) {
            INVALID_HBA_LINE0("end-of-line before IP address specification", search_in_comment, line_num);
            return NULL;
        }

        if (tokens->length > 1) {
            INVALID_HBA_LINE0("multiple values specified for host address", search_in_comment, line_num);
            free_tokens(tokens);
            tokens = NULL;
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
            char* str = NULL;
            errno_t rc = 0;

            /* IP and netmask are specified */
            parsedline->ip_cmp_method = ipCmpMask;

            /* need a modifiable copy of token */
            str = xstrdup(token->string);

            /* Check if it has a CIDR suffix and if so isolate it */
            cidr_slash = strchr(str, '/');
            /* Database Security: Disable remote connection with trust method. */
            if (NULL != cidr_slash)
                *cidr_slash = '\0';
            rc = strncpy_s(tmp_str, MAXLISTEN, str, MAXLISTEN - 1);
            securec_check_c(rc, "\0", "\0");
            tmp_str[MAXLISTEN - 1] = '\0';
            conntip = strstr(tmp_str, "/");
            /* verify the IP address */
            if (verify_ip(tmp_str))
                isip = true;

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
            if (ret == 0 && (gai_result != NULL)) {
                rc = memcpy_s(&parsedline->addr, sizeof(parsedline->addr), gai_result->ai_addr, gai_result->ai_addrlen);
                securec_check_c(rc, "\0", "\0");
            } else if (ret == EAI_NONAME)
                parsedline->hostname = xstrdup(str);
            else {
                INVALID_HBA_LINE2("invalid IP address \"%s\": %s",
                        str, gai_strerror(ret), search_in_comment, line_num);
                GS_FREE(str);
                if (NULL != gai_result)
                    pg_freeaddrinfo_all(gai_result);

                free_tokens(tokens);
                tokens = NULL;
                return NULL;
            }
            pg_freeaddrinfo_all(gai_result);

            /* Get the netmask */
            if (NULL != cidr_slash) {
                if (parsedline->hostname != NULL) {
                    INVALID_HBA_LINE1("specifying both host name and "
                                      "CIDR mask is invalid: \"%s\"",
                        token->string,
                        search_in_comment,
                        line_num);
                    free_tokens(tokens);
                    tokens = NULL;
                    GS_FREE(str);
                    return NULL;
                }

                if (pg_sockaddr_cidr_mask(&parsedline->mask, cidr_slash + 1, parsedline->addr.ss_family) < 0) {
                    INVALID_HBA_LINE1(
                        "invalid CIDR mask in address \"%s\"", token->string, search_in_comment, line_num);
                    free_tokens(tokens);
                    tokens = NULL;
                    GS_FREE(str);
                    return NULL;
                }
            } else if (parsedline->hostname == NULL) {
                /* Read the mask field. */
                free_tokens(tokens);
                tokens = next_field_expand(line, &cur_line_pos);
                if (NIL == tokens) {
                    INVALID_HBA_LINE0("end-of-line before netmask specification", search_in_comment, line_num);
                    GS_FREE(str);
                    return NULL;
                }
                if (tokens->length > 1) {
                    INVALID_HBA_LINE0("multiple values specified for netmask", search_in_comment, line_num);
                    free_tokens(tokens);
                    tokens = NULL;
                    GS_FREE(str);
                    return NULL;
                }
                token = (HbaToken*)linitial(tokens);

                ret = pg_getaddrinfo_all(token->string, NULL, &hints, &gai_result);
                if (ret || (gai_result == NULL)) {
                    INVALID_HBA_LINE2(
                        "invalid IP mask \"%s\": %s", token->string, gai_strerror(ret), search_in_comment, line_num);
                    if (NULL != gai_result)
                        pg_freeaddrinfo_all(gai_result);
                    free_tokens(tokens);
                    tokens = NULL;
                    GS_FREE(str);
                    return NULL;
                }

                rc = memcpy_s(&parsedline->mask, sizeof(parsedline->mask), gai_result->ai_addr, gai_result->ai_addrlen);
                securec_check_c(rc, "\0", "\0");
                pg_freeaddrinfo_all(gai_result);

                if (parsedline->addr.ss_family != parsedline->mask.ss_family) {
                    INVALID_HBA_LINE0("IP address and mask do not match", search_in_comment, line_num);
                    free_tokens(tokens);
                    tokens = NULL;
                    GS_FREE(str);
                    return NULL;
                }
            }
            GS_FREE(g_hbaAddr);
            g_hbaAddr = xstrdup(str);

            GS_FREE(str);
        }
        free_tokens(tokens);
        tokens = NULL;
    } /* != ctLocal */

    /* after verify: conntype+database+username+ip */
    if (isconntype && isdatabase && isroles && isip) {
        write_stderr("Notice: the above configuration uses cluster internal communication, your configuration may "
                     "affect the cluster internal communication.\n");
    }

    /* Get the authentication method */
    tokens = next_field_expand(line, &cur_line_pos);
    if (NIL == tokens) {
        if (line_num == 0) {
            parsedline->auth_method = uaImplicitReject;
            return parsedline;
        }

        INVALID_HBA_LINE0("end-of-line before authentication method", search_in_comment, line_num);
        return NULL;
    }

    if (tokens->length > 1) {
        INVALID_HBA_LINE0("multiple values specified for authentication type", search_in_comment, line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }
    token = (HbaToken*)linitial(tokens);

    unsupauth = NULL;
    /* Database Security:  Disable remote connection with trust method. */
    if (strncmp(token->string, "trust", sizeof("trust")) == 0) {
        parsedline->auth_method = uaTrust;
    } else if (strncmp(token->string, "ident", sizeof("ident")) == 0)
#ifdef USE_IDENT
        parsedline->auth_method = uaIdent;
#else
        unsupauth = "ident";
#endif
    else if (strncmp(token->string, "peer", sizeof("peer")) == 0)
        parsedline->auth_method = uaPeer;
    else if (strncmp(token->string, "krb5", sizeof("krb5")) == 0)
#ifdef KRB5
        parsedline->auth_method = uaKrb5;
#else
        unsupauth = "krb5";
#endif
    else if (strncmp(token->string, "gss", sizeof("gss")) == 0)
#ifdef ENABLE_GSS
        parsedline->auth_method = uaGSS;
#else
        unsupauth = "gss";
#endif
    else if (strncmp(token->string, "sspi", sizeof("sspi")) == 0)
#ifdef ENABLE_SSPI
        parsedline->auth_method = uaSSPI;
#else
        unsupauth = "sspi";
#endif
    else if (strncmp(token->string, "reject", sizeof("reject")) == 0)
        parsedline->auth_method = uaReject;
    else if (strncmp(token->string, "md5", sizeof("md5")) == 0) {
        parsedline->auth_method = uaMD5;
    }
    /* Database Security:  Support SHA256.*/
    else if (strncmp(token->string, "sha256", sizeof("sha256")) == 0) {
        parsedline->auth_method = uaSHA256;
    } else if (strncmp(token->string, "sm3", sizeof("sm3")) == 0) {
        parsedline->auth_method = uaSM3;
    } else if (strncmp(token->string, "pam", sizeof("pam")) == 0)
#ifdef USE_PAM
        parsedline->auth_method = uaPAM;
#else
        unsupauth = "pam";
#endif
    else if (strncmp(token->string, "ldap", sizeof("ldap")) == 0)
#ifdef USE_LDAP
        parsedline->auth_method = uaLDAP;
#else
        unsupauth = "ldap";
#endif
    else if (strncmp(token->string, "cert", sizeof("cert")) == 0)
#ifdef USE_SSL
        parsedline->auth_method = uaCert;
#else
        unsupauth = "cert";
#endif
    else {
        INVALID_HBA_LINE1("invalid authentication method \"%s\"", token->string, search_in_comment, line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    if (NULL != unsupauth) {
        INVALID_HBA_LINE1("invalid authentication method \"%s\": "
                          "not supported by this build",
            token->string,
            search_in_comment,
            line_num);
        free_tokens(tokens);
        tokens = NULL;
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
        INVALID_HBA_LINE0("krb5 authentication is not"
                          " supported on local sockets",
            search_in_comment,
            line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    if (parsedline->conntype == ctLocal && parsedline->auth_method == uaGSS) {
        INVALID_HBA_LINE0("gssapi authentication is not "
                          "supported on local sockets",
            search_in_comment,
            line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    if (parsedline->conntype != ctLocal && parsedline->auth_method == uaPeer) {
        INVALID_HBA_LINE0("peer authentication is only"
                          " supported on local sockets",
            search_in_comment,
            line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    /*
     * SSPI authentication can never be enabled on ctLocal connections,
     * because it's only supported on Windows, where ctLocal isn't supported.
     */

    if (parsedline->conntype != ctHostSSL && parsedline->auth_method == uaCert) {
        INVALID_HBA_LINE0("cert authentication is only"
                          " supported on hostssl connections",
            search_in_comment,
            line_num);
        free_tokens(tokens);
        tokens = NULL;
        return NULL;
    }

    free_tokens(tokens);
    tokens = NULL;

    return parse_hba_line_options(line, line_num, parsedline, search_in_comment, &cur_line_pos);
}

static HbaLine* parse_hba_line_options(
    const char* line, int line_num, HbaLine* parsedline, bool search_in_comment, int* pcur_line_pos)
{
    List* tokens = NULL;
    ListCell* tokencell = NULL;
    HbaToken* token = NULL;

    /* Parse remaining arguments */
    for (;;) {
        tokens = next_field_expand(line, pcur_line_pos);
        if (NIL == tokens) {
            break;
        }

        foreach (tokencell, tokens) {
            char* val = NULL;
            char* str = NULL;
            token = (HbaToken*)lfirst(tokencell);

            str = xstrdup(token->string);
            val = strchr(str, '=');
            if (val == NULL) {
                GS_FREE(str);
                /*
                 * Got something that's not a name=value pair.
                 */
                INVALID_HBA_LINE1("authentication option "
                                  "not in name=value format: %s",
                    token->string,
                    search_in_comment,
                    line_num);
                free_tokens(tokens);
                tokens = NULL;
                return NULL;
            }

            *val++ = '\0'; /* str now holds "name", val holds "value" */
            if (!parse_hba_auth_opt(str, val, parsedline, line_num, search_in_comment)) {
                GS_FREE(str);
                /* parse_hba_auth_opt already logged the error message */
                free_tokens(tokens);
                tokens = NULL;
                return NULL;
            }
            GS_FREE(str);
        }

        free_tokens(tokens);
        tokens = NULL;
    }

#ifdef USE_LDAP
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
        if ((parsedline->ldapprefix != NULL) || (parsedline->ldapsuffix != NULL)) {
            if ((parsedline->ldapbasedn != NULL) || (parsedline->ldapbinddn != NULL) ||
                (parsedline->ldapbindpasswd != NULL) || (parsedline->ldapsearchattribute != NULL)) {
                INVALID_HBA_LINE0("cannot use ldapbasedn, ldapbinddn,"
                                  " ldapbindpasswd, or ldapsearchattribute"
                                  " together with ldapprefix",
                    search_in_comment,
                    line_num);
                return NULL;
            }
        } else if (parsedline->ldapbasedn == NULL) {
            INVALID_HBA_LINE0("authentication method \"ldap\" "
                              "requires argument \"ldapbasedn\", \"ldapprefix\", "
                              "or \"ldapsuffix\" to be set",
                search_in_comment,
                line_num);
            return NULL;
        }
    }
#endif

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
static bool parse_hba_auth_opt(char* name, const char* val, HbaLine* hbaline, int line_num, bool search_in_comment)
{
    if (strncmp(name, "map", sizeof("map")) == 0) {
        if (hbaline->auth_method != uaIdent && hbaline->auth_method != uaPeer && hbaline->auth_method != uaKrb5 &&
            hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI && hbaline->auth_method != uaCert)
            INVALID_AUTH_OPTION("map", gettext_noop("ident, peer, krb5, gssapi, sspi, and cert"));
        hbaline->usermap = xstrdup(val);
    } else if (strncmp(name, "clientcert", sizeof("clientcert")) == 0) {
        /*
         * Since we require ctHostSSL, this really can never happen on
         * non-SSL-enabled builds, so don't bother checking for USE_SSL.
         */
        if (hbaline->conntype != ctHostSSL) {
            INVALID_HBA_LINE0("clientcert can only be"
                              " configured for \"hostssl\" rows",
                search_in_comment,
                line_num);
            return false;
        }
        if (strncmp(val, "1", sizeof("1")) == 0) {
            hbaline->clientcert = true;
        } else {
            if (hbaline->auth_method == uaCert) {
                INVALID_HBA_LINE0("clientcert can not be set to 0"
                                  " when using \"cert\" authentication",
                    search_in_comment,
                    line_num);
                return false;
            }
            hbaline->clientcert = false;
        }
    } else if (strncmp(name, "pamservice", sizeof("pamservice")) == 0) {
        REQUIRE_AUTH_OPTION(uaPAM, "pamservice", "pam");
        hbaline->pamservice = xstrdup(val);
    }

#ifdef USE_LDAP
    else if (strncmp(name, "ldaptls", sizeof("ldaptls")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldaptls", "ldap");
        if (strncmp(val, "1", sizeof("1")) == 0)
            hbaline->ldaptls = true;
        else
            hbaline->ldaptls = false;
    } else if (strncmp(name, "ldapserver", sizeof("ldapserver")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapserver", "ldap");
        hbaline->ldapserver = xstrdup(val);
    } else if (strncmp(name, "ldapport", sizeof("ldapport")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapport", "ldap");
        hbaline->ldapport = atoi(val);
        if (hbaline->ldapport == 0) {
            INVALID_HBA_LINE1("invalid LDAP port number: \"%s\"", val, search_in_comment, line_num);
            return false;
        }
    } else if (strncmp(name, "ldapbinddn", sizeof("ldapbinddn")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbinddn", "ldap");
        hbaline->ldapbinddn = xstrdup(val);
    } else if (strncmp(name, "ldapbindpasswd", sizeof("ldapbindpasswd")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbindpasswd", "ldap");
        hbaline->ldapbindpasswd = xstrdup(val);
    } else if (strncmp(name, "ldapsearchattribute", sizeof("ldapsearchattribute")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapsearchattribute", "ldap");
        hbaline->ldapsearchattribute = xstrdup(val);
    } else if (strncmp(name, "ldapbasedn", sizeof("ldapbasedn")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapbasedn", "ldap");
        hbaline->ldapbasedn = xstrdup(val);
    } else if (strncmp(name, "ldapprefix", sizeof("ldapprefix")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapprefix", "ldap");
        hbaline->ldapprefix = xstrdup(val);
    } else if (strncmp(name, "ldapsuffix", sizeof("ldapsuffix")) == 0) {
        REQUIRE_AUTH_OPTION(uaLDAP, "ldapsuffix", "ldap");
        hbaline->ldapsuffix = xstrdup(val);
    } else if (strncmp(name, "krb_server_hostname", sizeof("krb_server_hostname")) == 0) {
        REQUIRE_AUTH_OPTION(uaKrb5, "krb_server_hostname", "krb5");
        hbaline->krb_server_hostname = xstrdup(val);
    }
#else
    else if (strncmp(name, "ldap", 4) == 0) {
        INVALID_HBA_LINE1("invalid authentication option \"%s\": "
                          "as authentication method \"ldap\" is not supported by this build",
            name,
            search_in_comment,
            line_num);
        return false;
    }
#endif
    else if (strncmp(name, "krb_realm", sizeof("krb_realm")) == 0) {
        if (hbaline->auth_method != uaKrb5 && hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI)
            INVALID_AUTH_OPTION("krb_realm", gettext_noop("krb5, gssapi, and sspi"));
        hbaline->krb_realm = xstrdup(val);
    } else if (strncmp(name, "include_realm", sizeof("include_realm")) == 0) {
        if (hbaline->auth_method != uaKrb5 && hbaline->auth_method != uaGSS && hbaline->auth_method != uaSSPI)
            INVALID_AUTH_OPTION("include_realm", gettext_noop("krb5, gssapi, and sspi"));
        if (strncmp(val, "1", sizeof("1")) == 0)
            hbaline->include_realm = true;
        else
            hbaline->include_realm = false;
    } else {
        INVALID_HBA_LINE1("unrecognized authentication option name: \"%s\"", name, search_in_comment, line_num);
        return false;
    }
    return true;
}

void
obtain_parameter_list_array_for_hba(const char *flag, char *key, const char *value)
{
    int i = 0;

    for(i = 0; i < config_param_number; i++)
    {
        if (0 == strncmp(hba_param[i], flag,
                         strlen(hba_param[i]) > strlen(flag) ? strlen(hba_param[i]) : strlen(flag))) {
            GS_FREE(hba_param[i]);
            free_hba_line((HbaLine*)config_param[i]);
            GS_FREE(config_param[i]);
            GS_FREE(config_value[i]);

            hba_param[i] = xstrdup(flag);
            config_param[i] = key;
            if (NULL != value) {
                config_value[i] = xstrdup(value);
            } else {
                config_value[i] = NULL;
            }

            return;
        }
    }

    hba_param[i] = xstrdup(flag);
    config_param[config_param_number++] = key;
    if (NULL != value) {
        config_value[config_value_number++] = xstrdup(value);
    } else {
        config_value[config_value_number++] = NULL;
    }
}

void do_hba_analysis(const char* strcmd)
{
    HbaLine *hbaline = NULL;
    char    *hbaContext = NULL;
    size_t   len = 0;
    int      nRet = 0;
    hbaline = (HbaLine*)pg_malloc_zero(sizeof(HbaLine));

    if (NULL == parse_hba_line((char *)strcmd, 0, hbaline, false)){
        write_stderr(_("%s: Invalid hba configuration "
                       "\"%s\" option\n"),progname, strcmd);
        do_advice();
        exit(1);
    }

    trimBlanksTwoEnds((char **)&strcmd);

    if (NULL != g_hbaType)
        len += strlen(g_hbaType);
    if (NULL != g_hbaDatabase)
        len += strlen(g_hbaDatabase);
    if (NULL != g_hbaUser)
        len += strlen(g_hbaUser);
    if (NULL != g_hbaAddr)
        len += strlen(g_hbaAddr);

    if (0 == len){
        config_param[config_param_number++] = (char *)hbaline;
        config_value[config_value_number++] = xstrdup(strcmd);
    }else{
        hbaContext = (char *)pg_malloc_zero(sizeof(char)*(len + 1));
        nRet = snprintf_s(hbaContext, len + 1, len, "%s%s%s%s",
                          (NULL != g_hbaType ? g_hbaType : ""),
                          (NULL != g_hbaDatabase ? g_hbaDatabase : ""),
                          (NULL != g_hbaUser ? g_hbaUser : ""),
                          (NULL != g_hbaAddr ? g_hbaAddr : ""));
        securec_check_ss_c(nRet, hbaContext, "\0");
        obtain_parameter_list_array_for_hba(hbaContext, (char *)hbaline, strcmd);
        GS_FREE(hbaContext);
    }
    GS_FREE(g_hbaType);
    GS_FREE(g_hbaDatabase);
    GS_FREE(g_hbaUser);
    GS_FREE(g_hbaAddr);
    return;
}

char **
copy_file_context(char **opt_lines)
{
    int line_count = 0;
    int i = 0;
    char    **new_lines = NULL;

    for (i = 0; NULL != opt_lines[i]; i++)
    {
        line_count += 1;
    }

    new_lines = (char**)pg_malloc_zero((line_count+2) * sizeof(char *));
    for (i = 0; NULL != opt_lines[i]; i++)
    {
        new_lines[i] = xstrdup(opt_lines[i]);
    }

    return new_lines;
}

int do_hba_set(const char *action_type)
{
    char** opt_lines = NULL;
    char** new_lines = NULL;
    bool update_required = false;
    struct stat statbuf;
    struct stat tempbuf;
    int i = 0;
    int func_status = -1;
    int result_status = SUCCESS;

    FileLock filelock = {NULL,0};

    if (SUCCESS != check_config_file_status())
        return FAILURE;

    if (lstat(gucconf_file, &statbuf) == 0 && statbuf.st_size == 0
        && lstat(tempguc_file, &tempbuf) == 0 && tempbuf.st_size != 0){
        write_stderr(_("%s: the last signal is now,waiting....\n"), progname);
        return FAILURE;
    }

    if (get_file_lock(gucconf_lock_file,&filelock) != CODE_OK){
        return FAILURE;
    }

    /* .conf does not exists,.bak exist*/
    if (lstat(gucconf_file, &statbuf) != 0){
        opt_lines = backup_config_file(tempguc_file, gucconf_file, filelock, 3);
    }else{
        opt_lines = backup_config_file(gucconf_file, tempguc_file, filelock, 3);
    }

    if (NULL == opt_lines)
        return FAILURE;


    for (i = 0; i < config_param_number; i++)
    {
        if (((HbaLine*)config_param[i] == NULL) || (config_value[i] == NULL)){
            release_file_lock(&filelock);
            freefile(opt_lines);
	    freefile(new_lines);
            (void)write_stderr( _("%s: invalid input parameters\n"), progname);
            return FAILURE;
        }

        update_required = process_hba_line(opt_lines, (HbaLine*)config_param[i], config_value[i]);
        if (true == update_required){
            freefile(new_lines);
            new_lines = copy_file_context(opt_lines);
            freefile(opt_lines);
            opt_lines = copy_file_context(new_lines);
        }
    }

    if (NULL != new_lines){
        func_status = do_parameter_value_write(new_lines, UPDATE_PARAMETER);
        if (func_status != SUCCESS){
            result_status = FAILURE;
        }
    }

    if (SUCCESS == result_status){
        for (i = 0; i < config_param_number; i++){
            if (uaImplicitReject != ((HbaLine*)config_param[i])->auth_method){
                (void)write_stderr( "gs_guc %shba: %s: [%s]\n", action_type, config_value[i], gucconf_file);
            }else{
                (void)write_stderr( "gs_guc %shba: #%s: [%s]\n", action_type, config_value[i], gucconf_file);
            }
        }
    }

    release_file_lock(&filelock);
    freefile(opt_lines);
    freefile(new_lines);

    if (SUCCESS == result_status){
        return SUCCESS;
    }else{
        return FAILURE;
    }
}
