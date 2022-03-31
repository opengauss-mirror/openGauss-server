/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
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
 *  pg_guc.cpp
 *        the interface for user to set the guc parameters
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/pg_guc.cpp
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

#include "postgres_fe.h"
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "libpq/libpq-fe.h"
#include "flock.h"
#include "libpq/hba.h"

#include "libpq/pqsignal.h"
#include "getopt_long.h"
#include "miscadmin.h"
#include "cipher.h"

#include "bin/elog.h"
#include "openssl/rand.h"

#include "common/config/cm_config.h"
#if defined(__CYGWIN__)
#include <sys/cygwin.h>
#include <windows.h>
/* Cygwin defines WIN32 in windows.h, but we don't want it. */
#undef WIN32
#endif

#undef _
#define _(x) x

#define TRY_TIMES 3
#define LARGE_INSTANCE_NUM 2

#ifdef ENABLE_UT
#define static
#endif

/* PID can be negative for standalone backend */
typedef long pgpid_t;

/*
 *@@GaussDB@@
 *Brief           :update or add config_parameter
 *Description :
 */
typedef enum { UPDATE_PARAMETER = 0, ADD_PARAMETER } UpdateOrAddParameter;

/*
 *@@GaussDB@@
 *Brief           :the comand types
 *Description :
 */

typedef enum {
    NO_COMMAND = 0,
    SET_CONF_COMMAND,
    RELOAD_CONF_COMMAND,
    ENCRYPT_KEY_COMMAND,
    CHECK_CONF_COMMAND,
    GENERATE_KEY_COMMAND
} CtlCommand;

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

typedef enum {
    INSTANCE_ANY,
    INSTANCE_DATANODE,    /* postgresql.conf */
    INSTANCE_COORDINATOR, /* postgresql.conf */
    INSTANCE_GTM,         /* gtm.conf        */
    INSTANCE_GTM_PROXY,   /* gtm_proxy.conf  */
    INSTANCE_CMAGENT,     /* cm_agent.conf */
    INSTANCE_CMSERVER,    /* cm_server.conf */
} NodeType;

typedef struct {
    FILE* fp;
    size_t size;
} FileLock;

#define DEFAULT_WAIT 60
#define MAX_BUF_SIZE 4096
#define MAX_COMMAND_LEN 4096
#define MAX_CHILD_PATH 1024

static int sig = SIGHUP; /* default */
CtlCommand ctl_command = NO_COMMAND;
const char* progname;
char** config_param = {0};
char** hba_param = {0};
char** config_value = {0};
int node_type_value[5] = {0};
int config_param_number = 0;
int cndn_param_number = 0;
int cmserver_param_number = 0;
int cmagent_param_number = 0;
int gtm_param_number = 0;
int lc_param_number = 0;
int config_value_number = 0;
int node_type_number = 0;
int arraysize = 0;
char** cndn_param = {0};
char** gtm_param = {0};
char** cmserver_param = {0};
char** cmagent_param = {0};
char** lc_param = {0};

// Record the line information in the configuration file used by CN/DN/GTM/CM
char** cndn_guc_info = {0};
char** gtm_guc_info = {0};
char** cmserver_guc_info = {0};
char** cmagent_guc_info = {0};
char** lc_guc_info = {0};

bool is_hba_conf = false;
char pid_file[MAXPGPATH];
char gucconf_file[MAXPGPATH] = {0x00};
char tempguc_file[MAXPGPATH] = {0x00};
char gucconf_lock_file[MAXPGPATH] = {0x00};

const int MAX_PARAM_LEN = 1024;
const int MAX_VALUE_LEN = 1024;
#define INVALID_LINES_IDX (int)(~0)
const int EQUAL_MARK_LEN = 3;
#define MAX_CONFIG_ITEM 1024
#define PG_LOCKFILE_SIZE 1024
#define ADDPOINTER_SIZE 2
#define MAX_HOST_NAME_LENGTH 255

#define TEMP_PGCONF_FILE "postgresql.conf.bak"
#define TEMP_CMAGENTCONF_FILE "cm_agent.conf.bak"
#define TEMP_CMSERVERCONF_FILE "cm_server.conf.bak"
#define TEMP_GTMCONF_FILE "gtm.conf.bak"
#define TEMP_PGHBACONF_FILE "pg_hba.conf.bak"

#define DB_PROCESS_NAME "postgres"
#define PROG_NAME "gs_guc"
#define GUC_OPT_CONF_FILE "cluster_guc.conf"

/* execute result */
#define SUCCESS 0
#define FAILURE 1

// free the malloc memory
#define GS_FREE(ptr)            \
    do {                        \
        if (NULL != (ptr)) {    \
            free((char*)(ptr)); \
            ptr = NULL;         \
        }                       \
    } while (0)

// check the character value
#define IsIllegalCharacter(c) ((c) != '/' && !isdigit((c)) && !isalpha((c)) && (c) != '_' && (c) != '-')

NodeType nodetype = INSTANCE_ANY;
/* status which perform remote connection. default value is true, it means execute remote connection successfully */
bool g_remote_connection_signal = true;
/* result which perform remote command. default value is 0, it means execute remote command return code */
unsigned int g_remote_command_result = 0;

ErrCode retCode = CODE_OK;
KeyMode key_mode = UNKNOWN_KEY_MODE;
// user name
char* key_username = NULL;

// key text
char* g_plainkey = NULL;
// cipher key
char* g_cipherkey = NULL;
// the prefix of the output cipher/randfile
char* g_prefix = NULL;

// whether change the value of synchronous_standby_names
bool g_need_changed = true;
char* g_local_instance_path = NULL;

typedef struct {
    char** nodename_array;
    uint32 num;
} nodeInfo;
/* storage the name which perform remote connection failed */
nodeInfo* g_incorrect_nodeInfo = NULL;
/* storage the name which need to ignore */
nodeInfo* g_ignore_nodeInfo = NULL;

typedef struct {
    char** nodename_array;
    char** gucinfo_array;
    char** paramname_array;
    char** paramvalue_array;
    uint32 nodename_num;
    uint32 gucinfo_num;
    uint32 paramname_num;
    uint32 paramvalue_num;
} gucInfo;

/* real result */
gucInfo* g_real_gucInfo = NULL;
/* expect result */
gucInfo* g_expect_gucInfo = NULL;

uint32 g_local_dn_idx = 0;
char* g_current_data_dir = NULL;

void* pg_malloc(size_t size);
void* pg_malloc_zero(size_t size);

char* xstrdup(const char* s);
static void do_help(void);
static void do_help_config_guc(void);
static void do_help_check_guc(void);
static void do_help_config_hba(void);
static void do_help_encrypt(void);
static void do_help_generate(void);
static void do_help_common_options(void);
static void do_help_set_reset_options(void);
static void do_help_encrypt_options(void);
static void do_help_generate_options(void);
static bool is_file_exist(const char* path);
static void check_build_status(const char* path);
static bool is_gs_build_pid(const char* pid_path);

void do_advice(void);
static pgpid_t get_pgpid(void);
static int find_gucoption(
    char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len);
ErrCode writefile(char* path, char** lines, UpdateOrAddParameter isAddorUpdate);
void do_checkvalidate(int type);
int do_config_reload();
static int do_gucset(const char *action_type, const char *data_dir);
static char* do_guccheck(const char* param);
static void to_generatenewline(char* oldline, char* newline, const char* param, const char* value, int optvalue_len);
static int find_param_in_string(const char* name_string, const char* name_param, size_t param_len);
void free_space(char** optlines, int size);
void do_hba_analysis(const char* strcmd);
void free_hba_params();
static void checkArchivepath(const char* paraValue);
static bool isMatchOptionName(
    char* optLine, const char* paraName, int paraLength, int* paraOffset, int* valueLength, int* valueOffset);
bool isOptLineCommented(const char* optLine);
void trimBlanksTwoEnds(char** strPtr);
static void check_key_mode(const char* mode);
static void check_encrypt_options(void);
extern int do_hba_set(const char *action_type);
void get_instance_configfile(const char* datadir);
void print_gucinfo(const char* paraname);
int find_first_paravalue_index(const char* paraname);
int find_same_paravalue_index(const char* paraname);
void print_check_result();
int print_guc_result(const char* nodename);
int check_config_file_status();
char** backup_config_file(const char* read_file, char* write_file, FileLock filelock, int reserve_num_lines);
int do_parameter_value_write(char** opt_lines, UpdateOrAddParameter updateoradd);
static void checkCMParameter(const char* dataDir, const char* nodeName, const char* instName);
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

extern void freefile(char** lines);
extern char** readfile(const char* path, int reserve_num_lines);
extern int find_gucoption_available(
    char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len);
void process_cluster_guc_option(char* nodename, int type, char* instance_name, char* indatadir);
int validate_cluster_guc_options(char* nodename, int type, char* instance_name, char* indatadir);
extern int check_parameter(int type);
extern int save_guc_para_info();
extern bool get_env_value(const char* env_val, char* output_env_value, size_t env_var_value_len);
extern void check_env_value(const char* input_env_value);
bool allocate_memory_list();
extern int init_gauss_cluster_config(void);
extern char* get_AZ_value(char* value, const char* data_dir);
extern bool get_hostname_or_ip(char* out_name, size_t name_len);

#ifdef __cplusplus
}
#endif /* __cplusplus */

/*
 * @@GaussDB@@
 * Brief            : void *pg_malloc(size_t size)
 * Description      : malloc space
 * Notes            :
 */
void* pg_malloc(size_t size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        write_stderr(_("%s: malloc 0\n"), progname);
        exit(1);
    }

    result = (void*)malloc(size);
    if (NULL == result) {
        write_stderr(_("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}

/*
 * @@GaussDB@@
 * Brief            : void *pg_malloc_zero(size_t size)
 * Description      : malloc space, then set it to '\0'
 * Notes            :
 */
void* pg_malloc_zero(size_t size)
{
    void* tmp = NULL;
    errno_t rc = 0;

    tmp = pg_malloc(size);
    rc = memset_s(tmp, size, '\0', size);
    securec_check_c(rc, "\0", "\0");

    return tmp;
}

/*
 * @@GaussDB@@
 * Brief            : bool allocate_memory_list()
 * Description      :
 * Notes            :  Allocate memory space
 */
bool allocate_memory_list()
{
    FILE* fp = NULL;
    int lines = 0;
    char line_info[MAXPGPATH] = {0};
    char gausshome[MAXPGPATH] = {0};
    char guc_file[MAXPGPATH] = {0};
    int rc = 0;

    rc = memset_s(line_info, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");
    if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char)))
        return false;
    check_env_value(gausshome);
    rc = snprintf_s(guc_file, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, GUC_OPT_CONF_FILE);
    securec_check_ss_c(rc, "\0", "\0");

    if (checkPath(guc_file) != 0) {
        write_stderr(_("realpath(%s) failed : %s!\n"), guc_file, strerror(errno));
        return false;
    }
    /* maybe fail because of privilege*/
    fp = fopen(guc_file, "r");
    if (fp == NULL) {
        write_stderr("ERROR: Failed to open file\"%s\"\n", guc_file);
        return false;
    }
    if (NULL == fgets(line_info, MAXPGPATH - 1, fp)) {
        write_stderr("ERROR: Failed to read file\"%s\"\n", guc_file);
        fclose(fp);
        return false;
    }
    while ((fgets(line_info, MAXPGPATH - 1, fp)) != NULL) {
        /* get the lines of file */
        if ((int)strlen(line_info) > 0 && line_info[(int)strlen(line_info) - 1] == '\n')
            lines++;
    }
    if (0 != lines) {
        cndn_param = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        gtm_param = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        cmserver_param = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        cmagent_param = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        lc_param = ((char**)pg_malloc_zero(lines * sizeof(char*)));

        cndn_guc_info = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        gtm_guc_info = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        cmserver_guc_info = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        cmagent_guc_info = ((char**)pg_malloc_zero(lines * sizeof(char*)));
        lc_guc_info = ((char**)pg_malloc_zero(lines * sizeof(char*)));
    }

    fclose(fp);
    return true;
}

/*
 * @@GaussDB@@
 * Brief            : char *xstrdup(const char *s)
 * Description      :
 * Notes            :
 */
char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (NULL == result) {
        write_stderr(_("%s: out of memory\n"), progname);
        exit(1);
    }
    return result;
}
/*
 * @@GaussDB@@
 * Brief            : char* skipspace(char *p)
 * Description      : Skip all the blanks at the begin of p
 * Notes            :
 */
static char* skipspace(char *p)
{
    while (isspace((unsigned char)*p)) {
        p++;
    }
    return p;
}
/*
 * @@GaussDB@@
 * Brief            : static pgpid_t get_pgpid(void)
 * Description      : get pid number from pid file
 * Notes            :
 */
static pgpid_t get_pgpid(void)
{
    FILE* pidf = NULL;
    long pid;

    pidf = fopen(pid_file, "r");
    if (pidf == NULL) {
        /* No pid file, not an error on startup */
        if (errno == ENOENT)
            return 0;
        else {
            write_stderr(_("%s: could not open PID file \"%s\": %s\n"), progname, pid_file, gs_strerror(errno));
            exit(1);
        }
    }
    if (fscanf_s(pidf, "%ld", &pid) != 1) {
        write_stderr(_("%s: invalid data in PID file \"%s\"\n"), progname, pid_file);
        exit(1);
    }
    fclose(pidf);
    return (pgpid_t)pid;
}

static void check_path(const char *path_name)
{
    const char* danger_character_list[] = {"|",
        ";",
        "&",
        "$",
        "<",
        ">",
        "`",
        "\\",
        "'",
        "\"",
        "{",
        "}",
        "(",
        ")",
        "[",
        "]",
        "~",
        "*",
        "?",
        "!",
        "\n",
        NULL};
    int i = 0;
    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(path_name, danger_character_list[i]) != NULL) {
            fprintf(stderr, "invalid token \"%s\" in path name: (%s)\n", danger_character_list[i], path_name);
            exit(1);
        }
    }
}

/*
* @@GaussDB@@
* Brief            : static ErrCode writefile(char *path, char **lines,
                                              UpdateOrAddParameter isAddorUpdate)
* Description      : write record to the file
* Notes            : if failed, return the reason
*/
ErrCode writefile(char* path, char** lines, UpdateOrAddParameter isAddorUpdate)
{
    FILE* out_file = NULL;
    char** line = NULL;
    int fd = -1;
    if (UPDATE_PARAMETER == isAddorUpdate) {
        canonicalize_path(path);
        if ((out_file = fopen(path, "w")) == NULL) {
            (void)write_stderr(
                _("%s: could not open file \"%s\" for writing: %s\n"), progname, path, gs_strerror(errno));
            return CODE_OPEN_CONFFILE_FAILED;
        }

        fd = fileno(out_file);
        if ((fd >= 0) && (-1 == fchmod(fd, S_IRUSR | S_IWUSR))) {
            (void)write_stderr(_("could not set permissions of file  \"%s\"\n"), path);
        }
        rewind(out_file);

        for (line = lines; *line != NULL; line++) {
            if (fputs(*line, out_file) < 0) {
                (void)write_stderr(_("%s: could not write file \"%s\": %s\n"), progname, path, gs_strerror(errno));
                fclose(out_file);
                return CODE_WRITE_CONFFILE_ERROR;
            }
        }
    } else if (ADD_PARAMETER == isAddorUpdate) {
        canonicalize_path(path);
        out_file = fopen(path, "a+");
        if (NULL == out_file) {
            (void)write_stderr(
                _("%s: could not open file \"%s\" for writing: %s\n"), progname, path, gs_strerror(errno));
            return CODE_OPEN_CONFFILE_FAILED;
        }
        (void)fseek(out_file, 0, SEEK_END);
        if (fputs(*lines, out_file) < 0) {
            (void)write_stderr(_("%s: could not write file \"%s\": %s\n"), progname, path, gs_strerror(errno));
            fclose(out_file);
            return CODE_WRITE_CONFFILE_ERROR;
        }
    } else {
        return CODE_UNKOWN_ERROR;
    }

    /* fsync the out_file file immediately, in case of an unfortunate system crash.
     * We don't care about the result.
     */
    if (fsync(fileno(out_file)) != 0) {
        (void)fclose(out_file);
        (void)write_stderr(_("could not fsync file \"%s\": %s\n"), path, gs_strerror(errno));
        return CODE_WRITE_CONFFILE_ERROR;
    }

    if (fclose(out_file)) {
        (void)write_stderr(_("could not write file \"%s\": %s\n"), path, gs_strerror(errno));
        return CODE_CLOSE_CONFFILE_FAILED;
    }
    return CODE_OK;
}

/*
 * @@GaussDB@@
 * Brief            : find_param_in_string(char *name_string,const char* name_param,size_t param_len)
 * Description      : find the param in a line
 * Notes            :
 */
static int find_param_in_string(const char* name_string, const char* name_param, size_t param_len)
{
    char name_pri_temp[MAX_PARAM_LEN * 2] = {0};
    char name_new_temp[MAX_PARAM_LEN] = {0};
    char* cha = NULL;
    char* chb = NULL;
    int nRet = 0;

    nRet = snprintf_s(name_pri_temp, MAX_PARAM_LEN * 2, MAX_PARAM_LEN * 2 - 1, "%s", name_string);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = snprintf_s(name_new_temp, MAX_PARAM_LEN, MAX_PARAM_LEN - 1, "%s", name_param);
    securec_check_ss_c(nRet, "\0", "\0");

    cha = name_pri_temp;
    chb = name_new_temp;
    while (*cha) {
        if (*cha >= 'A' && *cha <= 'Z')
            *cha += 'a' - 'A';
        cha++;
    }
    while (*chb) {
        if (*chb >= 'A' && *chb <= 'Z')
            *chb += 'a' - 'A';
        chb++;
    }
    return strncmp(name_pri_temp, name_new_temp, param_len);
}
/*
* @@GaussDB@@
* Brief            : static void to_generatenewline(char* oldline, char* newline,
                                     const char* param, const char* value, int optvalue_len)
* Description      : according to the value from config file, make the new information
* Notes            :
*/
static void to_generatenewline(char* oldline, char* newline, const char* param, const char* value, int optvalue_len)
{
    char* poldline = NULL;
    char* pnewline = NULL;
    size_t paramlen = 0;
    size_t valuelen = 0;
    size_t offsetlen = 0;
    int rc = 0;

    if (NULL == oldline) {
        return;
    }
    poldline = oldline;
    pnewline = newline;
    paramlen = (size_t)strnlen(param, MAX_PARAM_LEN);
    valuelen = strnlen(value, MAX_VALUE_LEN);

    /* skill the blank */
    while (isspace((unsigned char)*poldline)) {
        rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
        securec_check_c(rc, "\0", "\0");
        poldline++;
        pnewline++;
        offsetlen++;
    }
    /* if the line is startwith '#', skip it. */
    if (*poldline == '#') {
        poldline++;
    }
    /* skill the blank */
    while (isspace((unsigned char)*poldline)) {
        rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
        securec_check_c(rc, "\0", "\0");
        poldline++;
        pnewline++;
        offsetlen++;
    }
    rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, paramlen);
    securec_check_c(rc, "\0", "\0");
    poldline += paramlen;
    pnewline += paramlen;
    offsetlen += paramlen;

    while (isspace((unsigned char)*poldline)) {
        rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
        securec_check_c(rc, "\0", "\0");
        poldline++;
        pnewline++;
        offsetlen++;
    }
    rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
    securec_check_c(rc, "\0", "\0");

    poldline++;
    pnewline++;
    offsetlen++;
    while (isspace((unsigned char)*poldline) && (unsigned char)*poldline != '\n') {
        rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
        securec_check_c(rc, "\0", "\0");
        poldline++;
        pnewline++;
        offsetlen++;
    }
    rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, value, valuelen);
    securec_check_c(rc, "\0", "\0");
    pnewline += valuelen;
    poldline += optvalue_len;
    offsetlen += valuelen;

    /*  After parameter assignment, the old postgresql.conf maybe have some other informations.
     *  Not copy them to pnewline, untill the alpha '#' appear .
     *  After this, don't forget the end signal '\0'.
     */
    while (*poldline && ('#' != *poldline)) {
        if (isspace((unsigned char)*poldline)) {
            rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
            securec_check_c(rc, "\0", "\0");
            poldline++;
            pnewline++;
            offsetlen++;
        } else {
            poldline++;
        }
    }
    if (*poldline == '#') {
        while (*poldline) {
            rc = strncpy_s(pnewline, MAX_VALUE_LEN * 2 - offsetlen, poldline, 1);
            securec_check_c(rc, "\0", "\0");
            poldline++;
            pnewline++;
            offsetlen++;
        }
    }

    /* copy the end signal */
    *pnewline = '\0';
}

/*
* @@GaussDB@@
* Brief            : static int find_gucoption(char** optlines, const char* opt_name,
                                            int* name_offset,   int* name_len,
                                            int* value_offset,  int* value_len)
* Description      : find the line information where the guc parameter in
* Notes            :
*/
static int find_gucoption(
    char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len)
{
    bool isMatched = false;
    int targetLine = 0;
    int matchTimes = 0;

    int i = 0;
    size_t paramlen = 0;

    if (NULL == optlines || NULL == opt_name) {
        return INVALID_LINES_IDX;
    }
    paramlen = (size_t)strnlen(opt_name, MAX_PARAM_LEN);
    if (NULL != name_len) {
        *name_len = (int)paramlen;
    }

    /* The first loop is to deal with the lines not commented by '#' */
    for (i = 0; optlines[i] != NULL; i++) {
        if (!isOptLineCommented(optlines[i])) {
            isMatched = isMatchOptionName(optlines[i], opt_name, paramlen, name_offset, value_len, value_offset);
            if (isMatched) {
                matchTimes++;
                targetLine = i;
            }
        }
    }

    if (matchTimes > 0) {
        if (matchTimes > 1) {
            (void)write_stderr("WARNING: There are %d \'%s\' not commented in \"postgresql.conf\", and only the "
                               "last one in %dth line will be set and used.\n",
                matchTimes,
                opt_name,
                (targetLine + 1));
        }

        return targetLine;
    }

    /* The second loop is to deal with the lines commented by '#' */
    matchTimes = 0;
    for (i = 0; optlines[i] != NULL; i++) {
        if (isOptLineCommented(optlines[i])) {
            isMatched = isMatchOptionName(optlines[i], opt_name, paramlen, name_offset, value_len, value_offset);
            if (isMatched) {
                matchTimes++;
                targetLine = i;
            }
        }
    }

    if (matchTimes > 0) {
        return targetLine;
    }

    return INVALID_LINES_IDX;
}

/*
 * @@GaussDB@@
 * Brief            : free the space
 * Description      :
 * Notes            :
 */
void free_space(char** optlines, int size)
{
    int i = 0;
    if (NULL == optlines) {
        return;
    }
    for (i = 0; i < size; i++) {
        if (optlines[i] != NULL) {
            GS_FREE(optlines[i]);
        }
    }
    GS_FREE(optlines);
}
/*
 * @@GaussDB@@
 * Brief            : get the file lock
 * Description      : lock postgresql.conf.lock file
 * Notes            :
 */
ErrCode get_file_lock(const char* path, FileLock* filelock)
{
    FILE* fp = NULL;
    struct stat statbuf;
    char content[PG_LOCKFILE_SIZE] = {0};
    int i = 0;
    int ret = -1;
    char newpath[MAXPGPATH] = {'\0'};

    if ((nodetype == INSTANCE_CMSERVER) ||
        (nodetype == INSTANCE_CMAGENT))
    {
        return CODE_OK;
    }
    if (NULL == path) {
        (void)write_stderr(_("%s: can not lock file: invalid path <NULL>"), progname);
        return CODE_UNKOWN_ERROR;
    }
    ret = strcpy_s(newpath, sizeof(newpath), path);
    securec_check_c(ret, "\0", "\0");
    canonicalize_path(newpath);
    if (checkPath(newpath) != 0) {
        write_stderr(_("realpath(%s) failed : %s!\n"), newpath, strerror(errno));
    }

    if (lstat(newpath, &statbuf) != 0) {
        fp = fopen(newpath, PG_BINARY_W);
        if (NULL == fp) {
            (void)write_stderr(
                _("%s: can't open lock file for write\"%s\" : %s\n"), progname, path, gs_strerror(errno));
            return CODE_OPEN_CONFFILE_FAILED;
        } else {
            if (fwrite(content, PG_LOCKFILE_SIZE, 1, fp) != 1) {
                fclose(fp);
                (void)write_stderr(_("%s: can't write lock file \"%s\" : %s\n"), progname, path, gs_strerror(errno));
                return CODE_WRITE_CONFFILE_ERROR;
            }
            fclose(fp);
        }
    }

    fp = fopen(newpath, PG_BINARY_RW);
    if (NULL == fp) {
        (void)write_stderr(_("could not open file or directory \"%s\", errormsg: %s\n"), path, gs_strerror(errno));
        return CODE_OPEN_CONFFILE_FAILED;
    }

    /* failed to lock file */
    while (i < TRY_TIMES) {
        ret = flock(fileno(fp), LOCK_EX | LOCK_NB, 0, START_LOCATION, statbuf.st_size);
        if (ret == -1) {
            i++;
            sleep(1); /* 1 sec */
            if (i >= TRY_TIMES) {
                (void)write_stderr(
                    _("could not lock file or directory \"%s\", errormsg: %s\n"), path, gs_strerror(errno));
                (void)write_stderr(_("HINT: This is transient error, as gaussdb is reading the configuration file. "
                                     "Please re-execute the command again.\n"));
                fclose(fp);
                return CODE_LOCK_CONFFILE_FAILED;
            }
            continue;
        }
        break;
    }
    filelock->fp = fp;
    filelock->size = statbuf.st_size;
    return CODE_OK;
}
/*
 * @@GaussDB@@
 * Brief            : release_file_lock(FileLock* filelock)
 * Description      : release file
 * Notes            :
 */
void release_file_lock(FileLock* filelock)
{
    if ((nodetype == INSTANCE_CMSERVER) ||
        (nodetype == INSTANCE_CMAGENT))
    {
        return;
    }

    /* release file */
    (void)flock(fileno(filelock->fp), LOCK_UN, 0, START_LOCATION, filelock->size);

    fclose(filelock->fp);
    filelock->fp = NULL;
    filelock->size = 0;
}

/*******************************************************************************
 Function    : check_config_file_status
 Description : check the status of postgresql.conf and postgresql.conf.bak
 Input       : None
 Output      : None
 Return      : int
             FAILURE - These two files are not present.
             SUCCESS - At least one file exists.
*******************************************************************************/
int check_config_file_status()
{
    struct stat statbuf;

    if ((lstat(gucconf_file, &statbuf) != 0) && (lstat(tempguc_file, &statbuf) != 0)) {
        char* pchBasename = strrchr(gucconf_file, '/');
        if (NULL == pchBasename) {
            pchBasename = gucconf_file;
        } else {
            pchBasename++;
        }

        write_stderr(_("%s: %s does not exist!\n"), progname, pchBasename);

        return FAILURE;
    }

    return SUCCESS;
}

/*******************************************************************************
 Function    : backup_config_file
 Description : backup the config file.
 Input       : read_file   - read file name
               write_file  - write file name
               filelock    - gucconf_lock_file
               reserve_num_lines - reserve num lines
 Output      : None
 Return      : opt_lines
               NULL        - read/write write_file failed
               opt_lines   - SUCCESS
*******************************************************************************/
char** backup_config_file(const char* read_file, char* write_file, FileLock filelock, int reserve_num_lines)
{
    char** opt_lines = NULL;
    ErrCode ret = CODE_OK;

    opt_lines = readfile(read_file, reserve_num_lines);
    if (NULL == opt_lines) {
        (void)write_stderr(_("read file \"%s\" failed: %s\n"), gucconf_file, gs_strerror(errno));
        release_file_lock(&filelock);
        return NULL;
    }

    ret = writefile(write_file, opt_lines, UPDATE_PARAMETER);
    if (ret != CODE_OK) {
        (void)write_stderr(_("could not write file \"%s\": %s\n"), write_file, gs_strerror(errno));
        release_file_lock(&filelock);
        freefile(opt_lines);
        return NULL;
    }

    return opt_lines;
}

/*******************************************************************************
 Function    : do_parameter_value_write
 Description : write the changed value into tempguc_file,
               then rename tempguc_file to gucconf_file
 Input       : opt_lines  - value information
               updateoradd - operation action
 Output      : None
 Return      : int
*******************************************************************************/
int do_parameter_value_write(char** opt_lines, UpdateOrAddParameter updateoradd)
{
    ErrCode ret = CODE_OK;
    int nRet = 0;
    char** newlines = NULL;
    FILE* fp = NULL;
    char newtempfile[MAXPGPATH * 2] = {0x00};

    nRet = snprintf_s(newtempfile, MAXPGPATH * 2, MAXPGPATH * 2 - 1, "%s_bak", tempguc_file);
    securec_check_ss_c(nRet, "\0", "\0");

    /* write bak file */
    ret = writefile(tempguc_file, opt_lines, updateoradd);
    if (ret != CODE_OK) {
        write_stderr("write file %s failed, errmsg: %s", tempguc_file, strerror(errno));
        return FAILURE;
    }

    /* write bak_bak file */
    newlines = readfile(tempguc_file, 0);
    if (NULL == newlines) {
        write_stderr(_("read file \"%s\" failed: %s\n"), tempguc_file, gs_strerror(errno));
        return FAILURE;
    }
    ret = writefile(newtempfile, newlines, UPDATE_PARAMETER);
    freefile(newlines);
    if (ret != CODE_OK) {
        write_stderr(_("could not write file \"%s\": %s\n"), newtempfile, gs_strerror(errno));
        return FAILURE;
    }

    /* rename bak_bak */
    if (0 != rename(newtempfile, gucconf_file)) {
        write_stderr("error while moving file (%s to %s) : %s.\n", newtempfile, gucconf_file, strerror(errno));
        /* don't need to care about the result */
        (void)unlink(newtempfile);
        return FAILURE;
    }

    /* fsync the file_dest file immediately, in case of an unfortunate system crash */
    fp = fopen(gucconf_file, "r");
    if (NULL == fp) {
        write_stderr(_("could not open file \"%s\", errormsg: %s\n"), gucconf_file, gs_strerror(errno));
        return FAILURE;
    }
    if (fsync(fileno(fp)) != 0) {
        (void)fclose(fp);
        write_stderr(_("could not fsync file \"%s\": %s\n"), gucconf_file, gs_strerror(errno));
        return FAILURE;
    }
    if (fclose(fp)) {
        write_stderr(_("could not write file \"%s\": %s\n"), gucconf_file, gs_strerror(errno));
        return FAILURE;
    }

    return SUCCESS;
}

bool
get_global_local_node_name()
{
    // check and assign values to global variables g_local_node_name. get_AZ_value must use it.
    if (NULL == g_local_node_name || '\0' == g_local_node_name[0]) {
        char    hostname[MAX_HOST_NAME_LENGTH] = {0};
        int     nRet = 0;
        nRet = memset_s(hostname, MAX_HOST_NAME_LENGTH, '\0', MAX_HOST_NAME_LENGTH);
        securec_check_c(nRet, "\0", "\0");

        (void)gethostname(hostname, MAX_HOST_NAME_LENGTH);
        if ('\0' == hostname[0]) {
            write_stderr("ERROR: Failed to get local hostname.\n");
            return FAILURE;
        }
        g_local_node_name = xstrdup(hostname);
    }
    return SUCCESS;
}

char **
append_string_info(char **optLines, const char *newContext)
{
    int  n = 0;
    errno_t ret = 0;
    while (NULL != optLines[n]) {
        n += 1;
    }
    char** optLinesResult = (char**)pg_malloc_zero(sizeof(char *) * (n + 2));
    ret = memcpy_s(optLinesResult, sizeof(char *) * (n + 2), optLines, sizeof(char *) * (n + 1));
    securec_check_c(ret, "\0", "\0");
    GS_FREE(optLines);

    optLinesResult[n] = xstrdup(newContext);
    optLinesResult[n + 1] = NULL;

    return optLinesResult;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 * For a line in the configuration file, skip current spaces.
 */
static void SkipSpace(char* &line)
{
    while (line != NULL && isspace((unsigned char)*line)) {
        ++line;
    }
}

/*
 * Handle commented lines in the configuration file.
 * return:
 * true - replconninfoX is commented
 * false - replconninfoX is not in current line.
 */
static bool ProcessCommented(char* &line, char* replconninfoX)
{
    ++line;
    SkipSpace(line);
    /* replconninfoX must be invalid if it is commented*/
    if (line != NULL && strncmp(line, replconninfoX, strlen(replconninfoX)) == 0) {
        return true;
    }
    return false;
}

/*
 * Handle lines which is not commented in the configuration file.
 * notNullReplconninfoNums - the number of replconninfo which is not null
 * matchReplconninfoX - whether replconninfoX is in current line.
 * isReplconninfoXNull - whether replconninfoX is NULL.
 */
static void ProcessNotCommented(char* &line, char* replconninfoX,
    int &notNullReplconninfoNums, bool &isReplconninfoXNull, bool &matchReplconninfoX)
{
    if (line != NULL && strncmp(line, "replconninfo", strlen("replconninfo")) == 0) {
        if (strncmp(line, replconninfoX, strlen(replconninfoX)) == 0) {
            matchReplconninfoX = true;
        }
        line += strlen(replconninfoX);
        /* Skip all the blanks between the param and '=' */
        SkipSpace(line);
        /* Skip '=' */
        if (line != NULL && *line == '=') {
            line++;
        }
        /* Skip all the blanks between the '=' and value */
        SkipSpace(line);
        if (line != NULL && strncmp(line, "''", strlen("''")) != 0 &&
            strncmp(line, "\"\"", strlen("\"\"")) != 0) {
            ++notNullReplconninfoNums;
            if (matchReplconninfoX) {
                isReplconninfoXNull = false;
            }
        }
    }
}

/*******************************************************************************
 Function    : IsLastNotNullReplconninfo
 Description : determine if replconninfoX which is being set is the last one valid replconninfo
 Input       : optLines  - postgres.conf info before changing
               replconninfoX - replconninfo param name which is being set, eg "replconninfo1"
 Output      : None
 Return      : bool
*******************************************************************************/
static bool IsLastNotNullReplconninfo(char** optLines, char* replconninfoX)
{
    int notNullReplconninfoNums = 0;
    bool isReplconninfoXNull = true;
    bool matchReplconninfoX = false;
    char* p = NULL;

    for (int i = 0; optLines != NULL && optLines[i] != NULL; i++) {
        p = optLines[i];
        /* Skip all the blanks at the begin of the optLine */
        SkipSpace(p);
        if (p == NULL) {
            continue;
        }
        if (*p == '#') {
            if(ProcessCommented(p, replconninfoX)) {
                return false;
            } else {
                continue;
            }
        }
        ProcessNotCommented(p, replconninfoX, notNullReplconninfoNums,
                            isReplconninfoXNull, matchReplconninfoX);
        if (notNullReplconninfoNums > 1) {
            return false;
        }
    }
    /* return true if replconninfoX which is being set is the last one valid replconninfo */
    if (notNullReplconninfoNums == 1 && !isReplconninfoXNull) {
        return true;
    }
    return false;
}

static void CheckLastValidReplconninfo(char** opt_lines, int idx)
{
    /* Give a warning if the last valid replconninfo is set to a invalid value currently */
    if (strncmp(config_param[idx], "replconninfo", strlen("replconninfo")) == 0 &&
        config_value[idx] != NULL && (strlen(config_value[idx]) == 0 ||
        strncmp(config_value[idx], "''", strlen("''")) == 0) &&
        IsLastNotNullReplconninfo(opt_lines, config_param[idx])) {
        write_stderr("\nWARNING: This is the last valid replConnInfo, once set to null, "
            "the host role will be changed to Normal if the local_role is primary now.\n");
    }
}

static void CheckKeepSyncWindow(char** opt_lines, int idx)
{
    /* Give a warning if keep_sync_window is set */
    if (strcmp(config_param[idx], "keep_sync_window") == 0) {
        write_stderr("\nWARNING: If the primary server fails during keep_sync_window, the transactions which "
            "were blocked during keep_sync_window will be lost and can't get recovered. This will affect RPO.\n");
    }
}

#endif

static void
parse_next_sync_groups(char **pgroup, char *result)
{
    if (**pgroup == '\0') {
        result[0] = '\0';
        return;
    }

    char *this_group = *pgroup;
    char *p = *pgroup;
    while (*p != '\0' && *p != ')') p++;
    while (*p != '\0' && *p != ',') p++;
    if (*p == ',') {
        *p = '\0';
       p++;
    }

    *pgroup = p;
    errno_t rc = snprintf_s(result, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "'%s'", this_group);
    securec_check_ss_c(rc, "\0", "\0");
    return;
}

static int
transform_az_name(char *config_value, char *allAZString, int allAZStringBufLen, const char *data_dir)
{
    if (strcmp(config_value, "''") == 0) {
        errno_t rc = strncpy_s(allAZString, allAZStringBufLen, config_value, strlen(config_value));
        securec_check_c(rc, "\0", "\0");
        return SUCCESS;
    }

    char    *azString = NULL;
    char    *buf = allAZString;
    int      buflen = allAZStringBufLen;

    char    *allgroup = xstrdup(config_value);
    char    *pgrp = allgroup + 1;                                  // trim first "'"
    char    this_group[MAX_VALUE_LEN] = {0x00};

    allgroup[strlen(allgroup) - 1] = '\0';                         // trim last  "'"
    parse_next_sync_groups(&pgrp, this_group);
    while (this_group[0] != '\0') {
        azString = get_AZ_value(this_group, data_dir);
        if (NULL == azString) {
            GS_FREE(allgroup);
            return FAILURE;
        }

        int azStringLen = strlen(azString);
        azString[0] = ' ';
        azString[azStringLen - 1] = ',';
        errno_t rc = strncpy_s(buf, buflen, azString, azStringLen);
        securec_check_c(rc, "\0", "\0");
        buf += azStringLen;
        buflen -= azStringLen;
        GS_FREE(azString);

        parse_next_sync_groups(&pgrp, this_group);
    }
    GS_FREE(allgroup);
    allAZString[0] = allAZString[strlen(allAZString) - 1] = '\'';

    return SUCCESS;
}

/*
 * @@GaussDB@@
 * Brief            :
 * Description      :
 * Notes            :
 */
static int
do_gucset(const char *action_type, const char *data_dir)
{
    char** opt_lines = NULL;
    int lines_index = 0;
    char* new_option = NULL;
    size_t new_optlen = 0;
    size_t newvalue_len = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    int nRet = 0;
    int rc = 0;
    int i = 0;

    char *tmpAZStr = NULL;
    char optconf_line[MAX_PARAM_LEN * 2] = {0x00};
    char newconf_line[MAX_PARAM_LEN * 2] = {0x00};
    struct stat statbuf;
    struct stat tempbuf;
    int func_status = -1;
    int result_status = SUCCESS;

    FileLock filelock = {NULL, 0};
    UpdateOrAddParameter updateoradd = UPDATE_PARAMETER;


    /* check the status of postgresql.conf and postgresql.conf.bak */
    if (SUCCESS != check_config_file_status())
        return FAILURE;

    if (lstat(gucconf_file, &statbuf) == 0 && statbuf.st_size == 0 &&
        lstat(tempguc_file, &tempbuf) == 0 && tempbuf.st_size != 0) {
        write_stderr(_("%s: the last signal is now,waiting....\n"), progname);
        return FAILURE;
    }

    /* lock gucconf_lock_file */
    if (get_file_lock(gucconf_lock_file, &filelock) != CODE_OK)
        return FAILURE;

    /* backup the config file. */
    /* .conf does not exists, .bak exist */
    if (lstat(gucconf_file, &statbuf) != 0)
        opt_lines = backup_config_file(tempguc_file, gucconf_file, filelock, 0);
    else
        opt_lines = backup_config_file(gucconf_file, tempguc_file, filelock, 0);

    if (NULL == opt_lines)
        return FAILURE;

    for (i = 0; i < config_param_number; i++)
    {
        if (NULL == config_param[i]) {
            release_file_lock(&filelock);
            freefile(opt_lines);
            GS_FREE(tmpAZStr);
            write_stderr( _("%s: invalid input parameters\n"), progname);
            return FAILURE;
        }

        // only when the parameter is synchronous_standby_names, this branch can be reached.
        if (g_need_changed && 0 == strncmp(config_param[i], "synchronous_standby_names",
                                           strlen(config_param[i]) > strlen("synchronous_standby_names") ? strlen(config_param[i]) : strlen("synchronous_standby_names"))) {
            // check and assign values to global variables g_local_node_name. get_AZ_value must use it.
            if (FAILURE == get_global_local_node_name()) {
                release_file_lock(&filelock);
                freefile(opt_lines);
                GS_FREE(tmpAZStr);
                return FAILURE;
            }

            // get AZ string
            if (NULL != config_value[i]) {
                char allAZString[MAX_VALUE_LEN] = {0x00};

                result_status = transform_az_name(config_value[i], allAZString, MAX_VALUE_LEN, data_dir);
                if (result_status == FAILURE) {
                    continue;
                }
                tmpAZStr = xstrdup(config_value[i]);

                GS_FREE(config_value[i]);
                config_value[i] = xstrdup(allAZString);
            }
        }

#ifndef ENABLE_MULTIPLE_NODES
        CheckLastValidReplconninfo(opt_lines, i);
        CheckKeepSyncWindow(opt_lines, i);
#endif

        /* find the line where guc parameter in */
        lines_index = find_gucoption(opt_lines, config_param[i], NULL, NULL, &optvalue_off, &optvalue_len);
        /* get the type of gs_guc execution */
        if (INVALID_LINES_IDX != lines_index) {
            /* Copy the original option line and substitute it with a new one */
            size_t line_len = 0;
            line_len = strlen(opt_lines[lines_index]);

            rc = strncpy_s(optconf_line, MAX_PARAM_LEN*2, opt_lines[lines_index], (size_t)Min(line_len, MAX_PARAM_LEN*2 - 1));
            securec_check_c(rc, "\0", "\0");

            if (NULL != config_value[i]) {
                to_generatenewline(optconf_line, newconf_line, config_param[i], config_value[i], optvalue_len);
            } else {
                /*
                 * if parameter as value is NULL; consider it as UNSET (i.e to default value)
                 *  which means comment the configuration parameter
                 */
                //line is commented
                if (isOptLineCommented(optconf_line)) {
                    rc = strncpy_s(newconf_line, MAX_PARAM_LEN*2, optconf_line, (size_t)Min(line_len, MAX_PARAM_LEN*2 - 1));
                    securec_check_c(rc, "\0", "\0");
                } else {
                    nRet = snprintf_s(newconf_line, MAX_PARAM_LEN*2, MAX_PARAM_LEN*2 - 1, "#%s", optconf_line);
                    securec_check_ss_c(nRet, "\0", "\0");
                }
            }
            updateoradd = UPDATE_PARAMETER;
        } else {
            /* If it does not find the guc parameter from parameter lists,
             * then add the guc parameter to the tail of config file
             */
            updateoradd= ADD_PARAMETER;
        }

        if (UPDATE_PARAMETER == updateoradd) {
            GS_FREE(opt_lines[lines_index]);
            opt_lines[lines_index] = xstrdup(newconf_line);
        } else {
            /* make a option line by new guc parameter value.
             * Total length = parameter_name_length + ' = ' + parameter_value_length + '\n'
             */
            size_t param_len=0;
            /*
             * if parameter not found and in case of default value skip the process.
             * so that server takes anyway default value.
             */
            if (NULL != config_value[i]) {
                newvalue_len = strnlen(config_value[i], MAX_VALUE_LEN);
                param_len = strnlen(config_param[i], MAX_VALUE_LEN);
                /*there is '\n', so length should add 1*/
                new_optlen = Min(param_len + EQUAL_MARK_LEN + newvalue_len + 1,(size_t)(MAX_VALUE_LEN-1) );
                new_option = (char *)pg_malloc_zero(new_optlen + 1);
                nRet = snprintf_s(new_option, (new_optlen + 1), new_optlen, "%s = %s\n", config_param[i], config_value[i]);
                securec_check_ss_c(nRet, "\0", "\0");
                new_option[new_optlen] = '\0';

                opt_lines = append_string_info(opt_lines, new_option);
                GS_FREE(new_option);
            }
        }
    }

    func_status = do_parameter_value_write(opt_lines, UPDATE_PARAMETER);
    if (SUCCESS == func_status) {
        for (i = 0; i < config_param_number; i++) {
            if (NULL != config_value[i]) {
                write_stderr( "gs_guc %s: %s=%s: [%s]\n", action_type, config_param[i], config_value[i], gucconf_file);
            } else {
                write_stderr( "gs_guc %s: #%s: [%s]\n", action_type, config_param[i], gucconf_file);
            }
        }
    } else {
        result_status = FAILURE;
    }

    release_file_lock(&filelock);
    freefile(opt_lines);

    for (i = 0; i < config_param_number; i++) {
        if (g_need_changed && 0 == strncmp(config_param[i], "synchronous_standby_names",
                                           strlen(config_param[i]) > strlen("synchronous_standby_names") ?
                                           strlen(config_param[i]) : strlen("synchronous_standby_names"))) {
            if (NULL != config_value[i] && tmpAZStr != NULL) {
                GS_FREE(config_value[i]);
                config_value[i] = xstrdup(tmpAZStr);
                GS_FREE(tmpAZStr);
            }
        }
    }

    if (SUCCESS == result_status) {
        return SUCCESS;
    } else {
        return FAILURE;
    }
}

/*************************************************************************************
 Function: do_guccheck
 Desc    : do gs_guc check option
           param  -    the parameter name
 Return  : char *
           NULL     it means that the guc file is not incorrect.
 *************************************************************************************/
static char* do_guccheck(const char* param)
{
    char** opt_lines = NULL;
    struct stat statbuf;
    int lines_index = 0;
    int optvalue_off = 0;
    int optvalue_len = 0;
    char* guc_value = NULL;
    int ret = 0;

    /* makesure the paramter value is not NULL. */
    if (NULL == param) {
        write_stderr(_("%s: invalid input parameters\n"), progname);
        return NULL;
    }

    /* check the status of postgresql.conf and postgresql.conf.bak */
    if (SUCCESS != check_config_file_status())
        return NULL;

    /* backup the config file. */
    /* .conf does not exists, .bak exist */
    if (lstat(gucconf_file, &statbuf) != 0)
        opt_lines = readfile(tempguc_file, 0);
    else
        opt_lines = readfile(gucconf_file, 0);

    if (NULL == opt_lines) {
        write_stderr(_("read file \"%s\" failed: %s\n"), gucconf_file, gs_strerror(errno));
        return NULL;
    }

    /* find the line where guc parameter in */
    lines_index = find_gucoption(opt_lines, param, NULL, NULL, &optvalue_off, &optvalue_len);

    guc_value = (char*)pg_malloc_zero(MAX_VALUE_LEN * sizeof(char));
    /*
     * if the parameter is not found and the option line starts with '#', the value is NULL.
     */
    if (INVALID_LINES_IDX != lines_index && !isOptLineCommented(opt_lines[lines_index])) {
        /* get the gs_guc value */
        ret = strncpy_s(guc_value,
            MAX_VALUE_LEN,
            (opt_lines[lines_index] + optvalue_off),
            Min((size_t)optvalue_len, MAX_VALUE_LEN - 1));
    } else {
        /*If we don't find the string, return "NoFound" to replace*/
        ret = strncpy_s(guc_value, MAX_VALUE_LEN, "NoFound", strlen("NoFound"));
    }
    securec_check_c(ret, "\0", "\0");

    freefile(opt_lines);
    return guc_value;
}

/*
 * @@GaussDB@@
 * Brief            : static void do_config_reload()
 * Description      : reload config file
 * Notes            :
 */
int do_config_reload()
{
    pgpid_t pid;

    pid = get_pgpid();
    if (pid == 0) /* no pid file */
    {
        write_stderr(_("%s: PID file \"%s\" does not exist\n"), progname, pid_file);
        write_stderr(_("Is server running?\n"));
        return FAILURE;
    } else if (pid < 0) /* standalone backend, not postmaster */
    {
        pid = -pid;
        write_stderr(_("%s: cannot reload server; "
                       "single-user server is running (PID: %ld)\n"),
            progname,
            pid);
        write_stderr(_("Please terminate the single-user server and try again.\n"));
        return FAILURE;
    }

    if (kill((pid_t)pid, sig) != 0) {
        write_stderr(
            _("%s: could not send reload signal (PID: %ld sig=%d): %s\n"), progname, pid, sig, gs_strerror(errno));
        return FAILURE;
    }

    write_stderr(_("server signaled\n"));
    return SUCCESS;
}

/*
 * @@GaussDB@@
 * Brief            :
 * Description      :
 * Notes            :
 */
void do_advice(void)
{
    printf(_("Try \"%s --help\" for more information.\n"), progname);
}

static void do_help(void)
{
    (void)printf(_("%s is an inferface to modify config files or encrypt plain text to cipher text.\n"), progname);
    do_help_check_guc();
    do_help_config_guc();
    do_help_config_hba();
    do_help_encrypt();
    do_help_generate();
    do_help_common_options();
    do_help_set_reset_options();
    do_help_encrypt_options();
    do_help_generate_options();
}

/*************************************************************************************
 Function: do_help_check_guc
 Desc    : function to print the help information about "gs_guc check"
 Return  :
 *************************************************************************************/
static void do_help_check_guc(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    (void)printf(_("\nChecking GUC parameters:\n"));

    (void)printf(_("    %s check -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} {-c \"parameter\", -c "
                   "\"parameter\", ...}\n"),
        progname);
    (void)printf(_("    %s check -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} {-c parameter, -c "
                   "parameter, ...}\n"),
        progname);
    (void)printf(_("\nOptions for check with -c parameter: \n"));
    (void)printf(_("  -Z NODE-TYPE   only can be \"coordinator\" or \"datanode\"\n"));
#else
#ifdef ENABLE_LITE_MODE
    (void)printf(_("\nChecking GUC parameters:\n"));

    (void)printf(_("    %s check [-Z NODE-TYPE] -D DATADIR {-c \"parameter\", -c "
                   "\"parameter\", ...}\n"), progname);
    (void)printf(_("    %s check [-Z NODE-TYPE] -D DATADIR {-c parameter, -c "
                   "parameter, ...}\n"), progname);
#else
    (void)printf(_("\nChecking GUC parameters:\n"));

    (void)printf(_("    %s check [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} {-c \"parameter\", -c "
                   "\"parameter\", ...}\n"),
        progname);
    (void)printf(_("    %s check [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} {-c parameter, -c "
                   "parameter, ...}\n"),
        progname);

#endif
#endif
}

static void do_help_config_guc(void)
{
    (void)printf(_("\nConfiguring GUC parameters:\n"));
    (void)printf(_("  Usage:\n"));
    
#ifdef ENABLE_MULTIPLE_NODES
    (void)printf(_("    NODE-TYPE is coordinator, datanode or gtm:\n"));
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--lcname=LCNAME] [--ignore-node=NODES] "
                   "{-c \"parameter = value\" -c \"parameter = value\" ...}\n"),
        progname);
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--lcname=LCNAME] [--ignore-node=NODES] "
                   "{-c \" parameter = value \" -c \" parameter = value \" ...}\n"),
        progname);
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--lcname=LCNAME] [--ignore-node=NODES] "
                   "{-c \"parameter = \'value\'\" -c \"parameter = \'value\'\" ...}\n"),
        progname);
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--lcname=LCNAME] [--ignore-node=NODES] "
                   "{-c \" parameter = \'value\' \" -c \" parameter = \'value\' \" ...}\n"),
        progname);
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--lcname=LCNAME] [--ignore-node=NODES] {-c \"parameter\" -c \"parameter\" ...}\n"),
        progname);
    (void)printf(_("    NODE-TYPE is cmserver or cmagent:\n"));
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N all -I all] "
                   "{-c \"parameter = value\" -c \"parameter = value\" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} -Z NODE-TYPE [-N all -I all] "
                   "{-c \" parameter = \'value\' \" -c \" parameter = \'value\' \" ...}\n"), progname);
    (void)printf(_("\n  If the parameter is a string variable, use the form: -c \"parameter = \'value\'\",\n"));
    (void)printf(_("    and if there is \" in the value, please transfer it with \\ carefully.\n"));
    (void)printf(
        _("    e.g. %s set -Z coordinator -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
        _("    e.g. %s set -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
        _("    e.g. %s set -Z coordinator -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"),
        progname);
    (void)printf(
        _("    e.g. %s reload -Z coordinator -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
        _("    e.g. %s reload -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(_("    e.g. %s reload -Z coordinator -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", "
                   "World\\!\'\".\n"),
        progname);
    (void)printf(
        _("    e.g. %s set -Z cmserver -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(_("    e.g. %s set -Z cmserver -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
        _("    e.g. %s set -Z cmagent -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(_("    e.g. %s set -Z cmagent -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
#else
#ifdef ENABLE_LITE_MODE
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \"parameter = value\" -c \"parameter = value\" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \" parameter = value \" -c \" parameter = value \" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \"parameter = \'value\'\" -c \"parameter = \'value\'\" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \" parameter = \'value\' \" -c \" parameter = \'value\' \" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--lcname=LCNAME] [--ignore-node=NODES] {-c \"parameter\" -c \"parameter\" ...}\n"), progname);
    (void)printf(
     _("    e.g. %s set -Z datanode -D /datanode/data -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
     _("    e.g. %s reload -Z datanode -D /datanode/data -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);

#else
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \"parameter = value\" -c \"parameter = value\" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \" parameter = value \" -c \" parameter = value \" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \"parameter = \'value\'\" -c \"parameter = \'value\'\" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--lcname=LCNAME] [--ignore-node=NODES] "
                "{-c \" parameter = \'value\' \" -c \" parameter = \'value\' \" ...}\n"), progname);
    (void)printf(_("        %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--lcname=LCNAME] [--ignore-node=NODES] {-c \"parameter\" -c \"parameter\" ...}\n"), progname);
    (void)printf(
     _("    e.g. %s set -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);
    (void)printf(
     _("    e.g. %s reload -Z datanode -N all -I all -c \"program = \'\\\"Hello\\\", World\\!\'\".\n"), progname);

#endif
#endif


    (void)printf(_("\n  If parameter value set or reload to DEFAULT OR COMMENT configuration parameter, use the form: "
                   "-c \"parameter\"\n"));
#ifdef ENABLE_MULTIPLE_NODES
    (void)printf(_("    and only \"coordinator\", \"datanode\" and \"gtm\" can be supported."));
#endif

    (void)printf(_("\n  You can choose Usage as you like, and perhaps the first one "));
    (void)printf(_("will be more suitable for you!\n"));
}

static void do_help_config_hba(void)
{
    (void)printf(_("\nConfiguring Authentication Policy:\n"));
    
#ifdef ENABLE_MULTIPLE_NODES
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] "
                   "-h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] "
                   "-h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] "
                   "-h \"HOSTTYPE DATABASE USERNAME HOSTNAME AUTHMEHOD authentication-options\" \n"),
        progname);

    (void)printf(_("  If authentication policy need to set/reload DEFAULT OR COMMENT then provide without "
                   "authentication menthod, use the form: \n"));
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK \" \n"),
        progname);
    (void)printf(_("    %s {set | reload} -Z NODE-TYPE [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                   "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME HOSTNAME\" \n"),
        progname);
#else
#ifdef ENABLE_LITE_MODE
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME HOSTNAME AUTHMEHOD authentication-options\" \n"),
        progname);

    (void)printf(_("  If authentication policy need to set/reload DEFAULT OR COMMENT then provide without "
                "authentication menthod, use the form: \n"));
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK \" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] -D DATADIR "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME HOSTNAME\" \n"),
        progname);
#else
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK AUTHMEHOD authentication-options\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] "
                "-h \"HOSTTYPE DATABASE USERNAME HOSTNAME AUTHMEHOD authentication-options\" \n"),
        progname);

    (void)printf(_("  If authentication policy need to set/reload DEFAULT OR COMMENT then provide without "
                "authentication menthod, use the form: \n"));
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR IPMASK\" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME IPADDR-WITH-IPMASK \" \n"),
        progname);
    (void)printf(_("    %s {set | reload} [-Z NODE-TYPE] [-N NODE-NAME] {-I INSTANCE-NAME | -D DATADIR} "
                "[--ignore-node=NODES] -h \"HOSTTYPE DATABASE USERNAME HOSTNAME\" \n"),
        progname);
#endif
#endif

}

static void do_help_encrypt(void)
{

    (void)printf(_("\nEncrypt plain text to cipher text:\n"));
    (void)printf(_("    %s encrypt [-M keymode] -K password [-U username] "
                   "{-D DATADIR | -R RANDFILEDIR -C CIPHERFILEDIR}\n"), progname);
}

static void do_help_generate(void)
{

    (void)printf(_("\nGenerate plain cipher key to cipher text:\n"));
    (void)printf(_("    %s generate [-o prefix] -S cipherkey -D DATADIR\n"), progname);
}

static void do_help_common_options(void)
{

    (void)printf(_("\nCommon options:\n"));
#ifndef ENABLE_LITE_MODE
    (void)printf(_("  -N                      nodename in which this command need to be executed\n"));
    (void)printf(_("  -I                      instance name\n"));
#endif
    (void)printf(_("  -D, --pgdata=DATADIR    location of the database storage area\n"));
    (void)printf(_("  -c    parameter=value   the parameter to set\n"));
    (void)printf(_("  -c    parameter         the parameter value to DEFAULT (i.e comments in configuration file)\n"));
    (void)printf(_("  --lcname=LCNAME         logical cluter name. It only can be used with datanode\n"));
    (void)printf(_("  --ignore-node=NODES     Nodes which need to ignore. "
                   "It only can be used with set/reload operation,and CN/DN nodetype\n"));
    (void)printf(_("  -h    host-auth-policy  to set authentication policy in HBA conf file\n"));
    (void)printf(_("  -?, --help              show this help, then exit\n"));
    (void)printf(_("  -V, --version           output version information, then exit\n"));
#ifdef ENABLE_MULTIPLE_NODES
    (void)printf(
        _("If the -D option is omitted, the environment variable PGDATA or GTMDATA is used based on NODE-TYPE.\n"));
#endif
}

static void do_help_set_reset_options(void)
{
#ifdef ENABLE_MULTIPLE_NODES

    (void)printf(_("\nOptions for set with -c parameter: \n"));
    (void)printf(_("  -Z NODE-TYPE   can be \"coordinator\", \"datanode\", \"cmserver\", \"cmagent\", or \"gtm\". "));

    (void)printf(_("NODE-TYPE is used to identify configuration file (with -c parameter) in data directory\n"));
    (void)printf(_("  \"coordinator\" or \"datanode\"                     -- postgresql.conf\n"));
    (void)printf(_("  \"coordinator\" and \"datanode\"                    -- postgresql.conf\n"));
    (void)printf(_("  \"cmserver\"                                      -- cm_server.conf\n"));
    (void)printf(_("  \"cmagent\"                                       -- cm_agent.conf\n"));
    (void)printf(_("  \"gtm\"                                           -- gtm.conf\n"));

    (void)printf(_("\nOptions for set and reload with -h host-auth-policy: \n"));
    (void)printf(_("  -Z NODE-TYPE   can be \"coordinator\", or \"datanode\"\n"));
#else

    (void)printf(_("\nOptions for set with -c parameter: \n"));
    (void)printf(_("  -Z NODE-TYPE   can only be \"datanode\", default is \"datanode\". "));

    (void)printf(_("NODE-TYPE is used to identify configuration file (with -c parameter) in data directory\n"));
    (void)printf(_("  \"datanode\"                     -- postgresql.conf\n"));

    (void)printf(_("\nOptions for set and reload with -h host-auth-policy: \n"));
    (void)printf(_("  -Z NODE-TYPE   can only be \"datanode\", default is \"datanode\"\n"));
#endif
}

static void do_help_encrypt_options(void)
{

    (void)printf(_("\nOptions for encrypt: \n"));
    (void)printf(_("  -M, --keymode=MODE     the cipher files will be applies in server, client or source,default "
                   "value is server mode\n"));
    (void)printf(_("  -K PASSWORD            the plain password you want to encrypt, which length should between 8~16 and at least 3 different types of characters\n"));
    (void)printf(_("  -U, --keyuser=USER     if appointed, the cipher files will name with the user name\n"));
    (void)printf(_("  -R RANDFILEDIR         set the dir that put the rand file\n"));
    (void)printf(_("  -C CIPHERFILEDIR       set the dir that put the cipher file\n"));
    (void)printf(_("\n"));
}

static void do_help_generate_options(void)
{

    (void)printf(_("\nOptions for generate: \n"));
    (void)printf(_("  -o PREFIX               the cipher files prefix. default value is obsserver\n"));
    (void)printf(_("  -S CIPHERKEY            the plain password you want to encrypt, which length should between 8~16 and at least 3 different types of characters\n"));
}

/*
 * @@GaussDB@@
 * Brief            :
 * Description      :
 * Notes            :
 */
static void checkArchivepath(const char* paraValue)
{
    char xlogarchpath[MAXPGPATH * 2] = {'\0'};
    char* endsp = NULL;

    int rc = 0;

    if (paraValue == NULL || *paraValue == 0) {
        write_stderr(_("%s:The archive command is empty.\n"), progname);
        exit(1);
    }

    if ((int)strlen(paraValue) + 1 > MAX_VALUE_LEN) {
        write_stderr(
            _("%s: The value of parameter archive_command is too long. Please check and make sure it is correct.\n"),
            progname);
        exit(1);
    }
    rc = snprintf_s(xlogarchpath, MAXPGPATH * 2, MAXPGPATH * 2 - 1, "%s", paraValue);
    securec_check_ss_c(rc, "\0", "\0");

    /* '%f' in archcommand */
    if ((endsp = strstr(xlogarchpath, "%f")) == NULL) {
        write_stderr(_("%s:The parameter \"archive_command\" should be set with \"%%f\".\n"), progname);
        exit(1);
    }
    /* '%p' in archcommand */
    if ((endsp = strstr(xlogarchpath, "%p")) == NULL) {
        write_stderr(_("%s:The parameter \"archive_command\" should be set with \"%%p\".\n"), progname);
        exit(1);
    }
}

static bool is_file_exist(const char* path)
{
    struct stat statbuf;
    bool isExist = true;

    if (lstat(path, &statbuf) < 0) {
        if (errno != ENOENT) {
            write_stderr(_("could not stat file \"%s\": %s\n"), path, strerror(errno));
            exit(1);
        }

        isExist = false;
    }
    return isExist;
}

/* check wheather the gs_build.pid contains content */
static bool is_gs_build_pid(const char* pid_path)
{
    FILE* pidf = NULL;
    long pid = 0;
    bool ret = false;

    pidf = fopen(pid_path, "r");
    if (pidf == NULL) {
        if (errno != ENOENT) {
            write_stderr(_("The file \"%s\" open failed: %s.\n"), pid_path, strerror(errno));
            exit(1);
        }
        goto fun_exit;
    }

    if (fscanf_s(pidf, "%ld", &pid) != 1) {
        goto fun_exit;
    }

fun_exit:
    if (pidf != NULL) {
        fclose(pidf);
        pidf = NULL;
    }

    if (pid != 0 && (kill(pid, 0) == 0)) {
        ret = true;
    }

    return ret;
}

/*
 * @@GaussDB@@
 * Brief            : static void check_build_status(const char* path)
 * Description      : check whether the build is starting
 * Notes            :
 */
static void check_build_status(const char* path)
{
    int ss_c = 0;
    char tmpfilename1[MAXPGPATH];
    char tmpfilename2[MAXPGPATH];

    ss_c = snprintf_s(tmpfilename1, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, "build_completed.start");
    securec_check_ss_c(ss_c, "", "");

    ss_c = snprintf_s(tmpfilename2, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, "gs_build.pid");
    securec_check_ss_c(ss_c, "", "");

    if (is_file_exist(tmpfilename1) || is_gs_build_pid(tmpfilename2)) {
        write_stderr(_("%s:The gs_ctl build is doing now, the gs_guc process should be stopped.\n"), progname);
        exit(1);
    }
}

/*
 * obtain_parameter_list_array
 *    During parameter parsing, the parameters with the same name will overwrite the previous parameters.
 * key   : the parameter name
 * value : the parameter value
 */
void
obtain_parameter_list_array(const char *key, const char *value)
{
    int i = 0;

    for(i = 0; i < config_param_number; i++)
    {
        if (0 == strncmp(config_param[i], key,
                         strlen(config_param[i]) > strlen(key) ? strlen(config_param[i]) : strlen(key))) {

            GS_FREE(config_param[i]);
            GS_FREE(config_value[i]);

            config_param[i] = xstrdup(key);
            if (NULL != value) {
                config_value[i] = xstrdup(value);
            } else {
                config_value[i] = NULL;
            }

            return;
        }
    }

    config_param[config_param_number++] = xstrdup(key);
    if (NULL != value) {
        config_value[config_value_number++] = xstrdup(value);
    } else {
        config_value[config_value_number++] = NULL;
    }
}


/*
 * @@GaussDB@@
 * Brief            : static void do_analysis(const char* strcmd)
 * Description      : save the parameter and value into array
 * Notes            :
 */
static void do_analysis(const char* strcmd)
{
    char* tempcmd = NULL;
    char* pcmd = NULL;
    if (strcmd == NULL) /*normally,it never happen*/
    {
        write_stderr(_("%s:the config command is wrong.\n"), progname);
        do_advice();
        exit(1);
    }
    if (config_param_number > arraysize || config_value_number > arraysize) /*normally,it never happen*/
    {
        write_stderr(_("%s: The number of config parameters:%d exceed the arraysize:%d\n"),
            progname,
            config_param_number,
            arraysize);
        do_advice();
        exit(1);
    }
    pcmd = (char*)strstr(strcmd, "=");
    /* Set the default value ---There is no '=' in 'strcmd' */
    if (NULL == pcmd) {
        trimBlanksTwoEnds((char**)&strcmd);
        if ((int)strlen(strcmd) > MAX_PARAM_LEN) {
            (void)write_stderr(
                _("%s: The parameter %s is too long. Please check and make sure it is correct.\n"), progname, strcmd);
            exit(1);
        }
        obtain_parameter_list_array(strcmd, NULL);
        return;
    }
    *pcmd = '\0';
    tempcmd = pcmd + 1;

    /* if  archivepath does not exist, exit */
    if (0 == strncmp(strcmd, "archive_command", 15)) {
        checkArchivepath(tempcmd);
    }

    /* BEGIN for gs_guc */
    trimBlanksTwoEnds((char**)&strcmd);
    trimBlanksTwoEnds(&tempcmd);
    /* END for gs_guc */

    if ((int)strlen(strcmd) > MAX_PARAM_LEN) {
        (void)write_stderr(
            _("%s: The parameter %s is too long. Please check and make sure it is correct.\n"), progname, strcmd);
        exit(1);
    }

    if (NULL != tempcmd && (int)strlen(tempcmd) > MAX_VALUE_LEN) {
        (void)write_stderr(_("%s: The value of parameter %s is too long. Please check and make sure it is correct.\n"),
            progname,
            strcmd);
        exit(1);
    }

    obtain_parameter_list_array(strcmd, tempcmd);
}

/*******************************************************************************
 Function    : do_check_parameter_name_type
 Description : check the type of parameter name
 Input       : None
 Output      : If the type is incorrect, do exit(1).
 Return      : void
*******************************************************************************/
void do_check_parameter_name_type()
{
    int i = 0;
    int j = 0;
    int name_len = 0;

    /* parameter must startwith alpha, 0-9 or '_' */
    for (i = 0; i < config_param_number; i++) {
        name_len = strnlen(config_param[i], MAX_PARAM_LEN);
        if ((*(config_param[i] + 0)) == '_' || *(config_param[i] + name_len - 1) == '_')
        /* startwith '_' and endwith '_' both are incorect */
        {
            write_stderr(_("The parameter(%s) exists illegal character:%c(or:%c)\n"),
                config_param[i],
                *(config_param[i] + 0),
                *(config_param[i] + name_len - 1));
            do_advice();
            exit(1);
        }
        for (j = 0; j < (int)strnlen(config_param[i], MAX_PARAM_LEN); j++) {
            if ((!isalpha((unsigned char)(*(config_param[i] + j)))) &&
                (!isdigit((unsigned char)(*(config_param[i] + j))))) {
                /* parameter must startwith alpha, 0-9 or '_' */
                if ((*(config_param[i] + j)) != '_') {
                    write_stderr(
                        _("The parameter(%s)exists illegal character:%c\n"), config_param[i], *(config_param[i] + j));
                    do_advice();
                    exit(1);
                }
            }
        }
    }
}

/*******************************************************************************
 Function    : do_check_parameter_value_type
 Description : check the type of parameter value
 Input       : None
 Output      : If the type is incorrect, do exit(1).
 Return      : void
*******************************************************************************/
void do_check_parameter_value_type()
{
    int i = 0;
    int j = 0;
    int num = 0;

    /* check the parameter value */
    for (i = 0; i < config_value_number; i++) {
        if (NULL == config_value[i]) {
            continue;
        }
        /* "gs_guc check" only support "parameter" not "parameter=value"*/
        if (ctl_command == CHECK_CONF_COMMAND) {
            write_stderr(_("The value of the parameter '-c' is incorrect.\n"));
            do_help_check_guc();
            exit(1);
        }

        if ((!isdigit((unsigned char)(*(config_value[i] + 0)))) && (*(config_value[i] + 0) != '\'') &&
            (!isalpha((unsigned char)(*(config_value[i] + 0)))) && (*(config_value[i] + 0) != '-')) {
            write_stderr(_("The parameter(%s) exists illegal character:%c\n"), config_value[i], *(config_value[i] + 0));
            do_advice();
            exit(1);
        }
        for (j = 0; j < (int)strnlen(config_value[i], MAX_PARAM_LEN); j++) {
            if (*(config_value[i] + j) == '\'') {
                num++;
            }
            if (*(config_value[i] + j) == '#') {
                write_stderr(
                    _("The parameter(%s) exists illegal character:%c \n"), config_value[i], *(config_value[i] + j));
                do_advice();
                exit(1);
            }
        }
        if (num == 1) {
            write_stderr(_("%s: the character '\'' can not make a pair or to many\n"), config_value[i]);
            do_advice();
            exit(1);
        }
    }
}
/*
 * @@GaussDB@@
 * Brief            : do_checkvalidate()
 * Description      : check the parameter name and value whether the style and value are both correct or not.
 * Notes            :
 */
void do_checkvalidate(int type)
{
    if (is_hba_conf)
        return;
    /* check the type of parameter name */
    do_check_parameter_name_type();
    /* check the type of parameter value */
    do_check_parameter_value_type();
    /* get guc list of CN/DN/CMSERVER/CMAGENT/GTM from cluster_guc.conf*/
    save_guc_para_info();
    /* check parameter name and value */
    if (0 != check_parameter(type)) {
        do_advice();
        exit(1);
    }
}

/*
 * @@GaussDB@@
 * Brief            : IsLegalPreix()
 * Input            : prefixStr -> the input parameter value
 * Description      : check the parameter. It is only support digit/alpha/'-'/'_'
 */
bool IsLegalPreix(const char* prefixStr)
{
    int NBytes = int(strlen(prefixStr));
    for (int i = 0; i < NBytes; i++) {
        /* check whether the character is correct */
        if (IsIllegalCharacter(prefixStr[i])) {
            return false;
        }
    }
    return true;
}

/*
 * do input parameter checking
 */

void checkDataDir(const char* datadir)
{
    if (NULL == datadir || 0 == strlen(datadir)) {
        write_stderr(_("%s: The value of -D is incorrect \n"), progname);
        exit(1);
    }
    if (-1 == access(datadir, R_OK | W_OK)) {
        write_stderr(_("ERROR: Could not access the path %s\n"), datadir);
        exit(1);
    }
}

void checkCipherkey()
{
    if (g_cipherkey == NULL) {
        g_cipherkey = simple_prompt("Password: ", MAX_KEY_LEN + 1, false);
        if (!check_input_password(g_cipherkey)) {
            write_stderr(_("%s: The input key must be %d~%d bytes and "
                "contain at least three kinds of characters!\n"),
                progname, MIN_KEY_LEN, MAX_KEY_LEN);
            do_advice();
            exit(1);
        }
    }
}

/*
 * @@GaussDB@@
 * Brief            : doGenerateKey()
 * Input            : datadir -> cipher/random file directory
 * Description      : generate cipher/rand file or generate encrypt file
 */
void doGenerateOperation(const char* datadir, const char* loginfo)
{
    /* check input parameter, make sure all is correct */
    checkDataDir(datadir);
    checkCipherkey();
    key_mode = OBS_MODE;

    if (strcmp(g_cipherkey, "default") == 0) {
        /* Default generate value of cipher key */
        char init_rand[RANDOM_LEN + 1] = {0};
        int retval = 0;
        char* encodetext = NULL;

        /* Generate a random value by OpenSSL function. */
        retval = RAND_priv_bytes((unsigned char*)init_rand, RANDOM_LEN);
        if (retval != 1) /* the return value of RAND_priv_bytes:1--success */
        {
            (void)write_stderr(_("%s: generate random key failed, errcode:%d.\n"), progname, retval);
            GS_FREE(g_cipherkey);
            exit(1);
        }

        encodetext = SEC_encodeBase64((char*)init_rand, RANDOM_LEN + 1);
        GS_FREE(g_cipherkey);
        g_cipherkey = (char*)encodetext;
        if (g_cipherkey == NULL) {
            (void)write_stderr(_("%s: encode Base64 failed\n"), progname);
            exit(1);
        }
        /* Cut 13 characters as cipher key */
        if (strlen(g_cipherkey) > RANDOM_LEN) {
            g_cipherkey[RANDOM_LEN - 3] = '\0';
        }
        gen_cipher_rand_files(key_mode, g_cipherkey, key_username, datadir, g_prefix);
        OPENSSL_free(g_cipherkey);
        g_cipherkey = NULL;
    } else {
        if (!check_input_password(g_cipherkey)) {
            write_stderr(_("%s: The input key must be %d~%d bytes and "
                "contain at least three kinds of characters!\n"),
                progname, MIN_KEY_LEN, MAX_KEY_LEN);
            GS_FREE(g_cipherkey);
            do_advice();
            exit(1);
        }
        gen_cipher_rand_files(key_mode, g_cipherkey, key_username, datadir, g_prefix);
        GS_FREE(g_cipherkey);
    }

    (void)write_log("gs_guc generate %s\n", loginfo);
}

void checkLcName(int nodeType)
{
    if ((NULL != g_lcname) && (INSTANCE_DATANODE != nodeType)) {
        write_stderr(_("%s: lcname only can be used with \"datanode\"\n"), progname);
        do_help_common_options();
        exit(1);
    }
}

void
print_help(const char* infoStr)
{
    if (strncmp(infoStr, "--help", sizeof("--help")) == 0 || strncmp(infoStr, "-?", sizeof("-?")) == 0) {
        do_help();
        exit(0);
    } else if (strncmp(infoStr, "-V", sizeof("-V")) == 0 || strncmp(infoStr, "--version", sizeof("--version")) == 0) {
#ifdef ENABLE_MULTIPLE_NODES
        puts("gs_guc " DEF_GS_VERSION);
#else
        puts("gs_guc (openGauss) " PG_VERSION);
#endif
        exit(0);
    }
}

void
get_action_value(CtlCommand ctlCmd, const char* cmd)
{
    if (ctlCmd != NO_COMMAND) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, cmd);
        do_advice();
        exit(1);
    } else if (strncmp(cmd, "set", sizeof("set")) == 0) {
        ctl_command = SET_CONF_COMMAND ;
    } else if (strncmp(cmd, "reload", sizeof("reload")) == 0) {
        ctl_command = RELOAD_CONF_COMMAND;
    } else if (strncmp(cmd, "encrypt", sizeof("encrypt")) == 0) {
        ctl_command = ENCRYPT_KEY_COMMAND;
    } else if (strncmp(cmd, "check", sizeof("check")) == 0) {
        ctl_command = CHECK_CONF_COMMAND;
    } else if (strncmp(cmd, "generate", sizeof("generate")) == 0) {
        ctl_command = GENERATE_KEY_COMMAND;
    } else {
        write_stderr(_("%s: unrecognized operation mode \"%s\"\n"), progname, cmd);
        do_advice();
        exit(1);
    }
}

void
get_node_type_info(const char *type)
{
    // the number of -Z is greater than 2
    if (node_type_number >= LARGE_INSTANCE_NUM) {
        (void)write_stderr("ERROR: The number of -Z must be less than or equal to 2.\n");
        do_advice();
        exit(1);
    }
    if (0 == strncmp("coordinator", type, sizeof("coordinator"))) {
        nodetype = INSTANCE_COORDINATOR;
        node_type_value[node_type_number++] = INSTANCE_COORDINATOR;
    } else if (0 == strncmp("datanode", type, sizeof("datanode"))) {
        nodetype = INSTANCE_DATANODE;
        node_type_value[node_type_number++] = INSTANCE_DATANODE;
    } else if (0 == strncmp("cmserver", type, sizeof("cmserver"))) {
        nodetype = INSTANCE_CMSERVER;
        node_type_value[node_type_number++] = INSTANCE_CMSERVER;
    } else if (0 == strncmp("cmagent", type, sizeof("cmagent"))) {
        nodetype = INSTANCE_CMAGENT;
        node_type_value[node_type_number++] = INSTANCE_CMAGENT;
    } else if (0 == strncmp("gtm", type, sizeof("gtm"))) {
        nodetype = INSTANCE_GTM;
        node_type_value[node_type_number++] = INSTANCE_GTM;
    } else {
        write_stderr(_("%s: unrecognized node type \"%s\"\n"), progname, type);
        do_advice();
        exit(1);
    }
}

void
check_ctl_command(CtlCommand ctlCmd, NodeType nodeType)
{
    if ((CHECK_CONF_COMMAND == ctlCmd) && (nodeType != INSTANCE_COORDINATOR) && (nodeType != INSTANCE_DATANODE)) {
        write_stderr(_("%s: check operation is only supported for \"coordinator\" and \"datanode\"\n"), progname);
        do_help_check_guc();
        exit(1);
    }
}

void clear_g_incorrect_nodeInfo()
{
    uint32 idx = 0;
    if (NULL == g_incorrect_nodeInfo) {
        return;
    }
    for (idx = 0; idx < g_incorrect_nodeInfo->num; idx++) {
        GS_FREE(g_incorrect_nodeInfo->nodename_array[idx]);
    }
    GS_FREE(g_incorrect_nodeInfo->nodename_array);
    GS_FREE(g_incorrect_nodeInfo);
}

static void clear_g_ignore_nodeInfo()
{
    uint32 idx = 0;
    if (g_ignore_nodeInfo == NULL) {
        return;
    }
    for (idx = 0; idx < g_ignore_nodeInfo->num; idx++) {
        GS_FREE(g_ignore_nodeInfo->nodename_array[idx]);
    }
    GS_FREE(g_ignore_nodeInfo->nodename_array);
    GS_FREE(g_ignore_nodeInfo);
}

static int countIgnoreNodeElems(const char *input)
{
    int cnt = 1;

    for (; *input != '\0'; input++) {
        if (*input == ',') {
            cnt++;
        }
    }
    return cnt;
}

static void saveIgnoreNodeElems(const char *input)
{
    errno_t rc = 0;
    int cnt = 0;
    int strLen = 0;
    char* node = NULL;
    char* tmpStr = NULL;
    char* saveStr = NULL;
    const char* split = ",";

    if ((input == NULL) || (input[0] == '\0')) {
        write_stderr(_("%s: invalid ignore nodes,please check it\n"), progname);
        exit(1);
    }

    strLen = strlen(input);
    tmpStr = (char*)pg_malloc_zero(strLen + 1);
    rc = memcpy_s(tmpStr, strLen, input, strLen);
    securec_check_c(rc, "\0", "\0");

    cnt = countIgnoreNodeElems(tmpStr);
    g_ignore_nodeInfo = (nodeInfo*)pg_malloc(sizeof(nodeInfo));
    g_ignore_nodeInfo->nodename_array = (char**)pg_malloc_zero(cnt * sizeof(char*));
    g_ignore_nodeInfo->num = 0;

    node = strtok_r(tmpStr, split, &saveStr);
    g_ignore_nodeInfo->nodename_array[g_ignore_nodeInfo->num++] = xstrdup(node);
    while (node != NULL) {
        node = strtok_r(NULL, split, &saveStr);
        if (node == NULL) {
            break;
        }
        g_ignore_nodeInfo->nodename_array[g_ignore_nodeInfo->num++] = xstrdup(node);
    }
    free(tmpStr);
}

static void process_encrypt_cmd(const char* pgdata_D, const char* pgdata_C, const char* pgdata_R)
{
    errno_t rc = 0;

    if (pgdata_D && (pgdata_R || pgdata_C)) {
        write_stderr(_("%s: encrypt cannot use -D, -R and -C same time!\n"), progname);
        do_advice();
        exit(1);
    }

    if (!pgdata_D && !(pgdata_R || pgdata_C)) {
        write_stderr(_("%s: encrypt must use -D or <-R and -C> !\n"), progname);
        do_advice();
        exit(1);
    }

    if ((pgdata_R && !pgdata_C) || (!pgdata_R && pgdata_C)) {
        write_stderr(_("%s: encrypt must use -R and -C same time!\n"), progname);
        do_advice();
        exit(1);
    }

    if (!pgdata_D) {
        pgdata_D = (char *)pgdata_C;
    }
    checkDataDir(pgdata_D);
    if (pgdata_R) {
        checkDataDir(pgdata_R);
    }
    gen_cipher_rand_files(key_mode, g_plainkey, key_username, pgdata_D, NULL);
    if (pgdata_R) {
        FILE* cmd_fp = NULL;
        char read_buf[MAX_CHILD_PATH] = {0};
        char* mv_cmd = NULL;

        mv_cmd = (char*)pg_malloc_zero(MAX_COMMAND_LEN + 1);
        rc = snprintf_s(mv_cmd, MAX_COMMAND_LEN, MAX_COMMAND_LEN - 1, "mv %s/*.rand %s/",
                        pgdata_C, pgdata_R);
        securec_check_ss_c(rc, "\0", "\0");
        cmd_fp = popen(mv_cmd, "r");
        if (NULL == cmd_fp) {
            (void)write_stderr("could not open mv command.\n");
            GS_FREE(mv_cmd);
            exit(1);
        }
        while (fgets(read_buf, sizeof(read_buf) - 1, cmd_fp) != 0) {
            printf("%s\n", read_buf);
            rc = memset_s(read_buf, sizeof(read_buf), 0, sizeof(read_buf));
            securec_check_c(rc, "\0", "\0");
        }
        pclose(cmd_fp);
        GS_FREE(mv_cmd);
    }
}

/*
 * @@GaussDB@@
 * Brief            :
 * Description      :
 * Notes            :
 */
int main(int argc, char** argv)
{

#define MAXLOGINFOLEN 1024
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
        {"version", no_argument, NULL, 'V'},
        {"pgdata", required_argument, NULL, 'D'},
        {"keyuser", required_argument, NULL, 'U'},
        {"keymode", required_argument, NULL, 'M'},
        {"lcname", required_argument, NULL, 1},
        {"ignore-node", required_argument, NULL, 2},
        {NULL, 0, NULL, 0}};

    int option_index;
    int c;
    char* temp_config_parameter = NULL;
    bool bhave_param = false;
    bool bhave_nodetype = false;
    char loginfo[MAXLOGINFOLEN] = {0};
#if defined(WIN32) || defined(__CYGWIN__)
    (void)setvbuf(stderr, NULL, _IONBF, 0);
#endif
    char* pgdata_D = NULL;
    char* pgdata_R = NULL;
    char* pgdata_C = NULL;
    char* nodename = NULL;
    char* instance_name = NULL;
    int nRet = 0;
    errno_t rc = 0;
    progname = PROG_NAME;
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_guc"));
    arraysize = argc - 1;
#ifndef ENABLE_LITE_MODE
    bool is_cluster_init = false;
#endif

    if (0 != arraysize) {
        config_param = ((char**)pg_malloc_zero(arraysize * sizeof(char*)));
        hba_param = ((char**)pg_malloc_zero(arraysize * sizeof(char *)));
        config_value = ((char**)pg_malloc_zero(arraysize * sizeof(char*)));
    }

    key_mode = SERVER_MODE;
    /*
     * save argv[0] so do_setgucparameter can look for the postmaster if necessary. we
     * don't look for postmaster here because in many cases we won't need it.
     */
    (void)umask(S_IRWXG | S_IRWXO);
    /* support --help and --version even if invoked as root */
    if (argc > 1) {
        print_help(argv[1]);
    }
#ifndef WIN32
    if (geteuid() == 0) {
        write_stderr(_("%s: cannot be run as root\n"
                       "Please log in (using, e.g., \"su\") as the "
                       "(unprivileged) user that will\n"
                       "own the server process.\n"),
            progname);
        exit(1);
    }
#endif
    optind = 1;
    /* process command-line options */
    while (optind < argc) {
        while ((c = getopt_long(argc, argv, "N:I:D:R:C:c:h:Z:U:M:K:S:o:", long_options, &option_index)) != -1) {
            switch (c) {
                case 'D': {
                    GS_FREE(pgdata_D);
                    pgdata_D = xstrdup(optarg);
                    canonicalize_path(pgdata_D);
                    break;
                }
                case 'R': {
                    GS_FREE(pgdata_R);
                    pgdata_R = xstrdup(optarg);
                    canonicalize_path(pgdata_R);
                    check_path(pgdata_R);
                    break;
                }
                case 'C': {
                    GS_FREE(pgdata_C);
                    pgdata_C = xstrdup(optarg);
                    canonicalize_path(pgdata_C);
                    check_path(pgdata_C);
                    break;
                }
                case 'o': {
                    if (!IsLegalPreix(optarg)) {
                        write_stderr(_("%s: -o only be formed of 'a~z', 'A~Z', '0~9', '-', '_'\n"), progname);
                        do_advice();
                        exit(1);
                    }
                    GS_FREE(g_prefix);
                    g_prefix = xstrdup(optarg);
                    break;
                }
                case 'c': {
                    if (is_hba_conf) {
                        write_stderr(_("%s: only one type of configuration can be changed at a time\n"
                                       "HINT: use either -c or -h option"),
                            progname);
                        do_advice();
                        exit(1);
                    }

                    if (strstr((const char*)optarg, "\n") != NULL) {
                        write_stderr("Error: The command line contains line breaks\n");
                        exit(1);
                    }

                    bhave_param = true;
                    temp_config_parameter = xstrdup(optarg);
                    do_analysis(temp_config_parameter);
                    GS_FREE(temp_config_parameter);
                    break;
                }
                case 'h': {
                    if (bhave_param) {
                        write_stderr(_("%s: only one type of configuration can be changed at a time\n"
                                       "HINT: use either -c or -h option"),
                            progname);
                        do_advice();
                        exit(1);
                    }
                    is_hba_conf = true;
                    temp_config_parameter = xstrdup(optarg);
                    // init cluster static config
#ifndef ENABLE_LITE_MODE
                    if (!is_cluster_init) {
                        init_gauss_cluster_config();
                        is_cluster_init = true;
                    }
#endif
                    do_hba_analysis(temp_config_parameter);
                    GS_FREE(temp_config_parameter);
                    break;
                }
                case 'Z': {
                    bhave_nodetype = true;
                    get_node_type_info(optarg);
                    break;
                }
                case 'N': {
                    GS_FREE(nodename);
                    nodename = xstrdup(optarg);
                    break;
                }
                case 'I': {
                    GS_FREE(instance_name);
                    instance_name = xstrdup(optarg);
                    break;
                }
                case 'M': {
                    size_t mlen = 0;
                    char* mode_str = NULL;
                    check_key_mode(optarg);
                    mlen = strlen("-M ") + strlen(optarg) + strlen(" ") + 1;
                    mode_str = (char*)pg_malloc_zero(mlen);
                    nRet = snprintf_s(mode_str, mlen, mlen - 1, "-M %s ", optarg);
                    securec_check_ss_c(nRet, mode_str, "\0");
                    rc = strncat_s(loginfo, MAXLOGINFOLEN, mode_str, mlen - 1);
                    securec_check_c(rc, "\0", "\0");

                    loginfo[MAXLOGINFOLEN - 1] = '\0';
                    GS_FREE(mode_str);
                    break;
                }
                case 'U': {
                    char name_str[MAX_KEY_LEN] = {0};
                    GS_FREE(key_username);
                    key_username = xstrdup(optarg);
                    nRet = snprintf_s(name_str, MAX_KEY_LEN, MAX_KEY_LEN - 1, "-U %s ", "***");
                    securec_check_ss_c(nRet, "\0", "\0");
                    rc = strncat_s(loginfo, MAXLOGINFOLEN, name_str, strlen(name_str));
                    securec_check_c(rc, "\0", "\0");

                    loginfo[MAXLOGINFOLEN - 1] = '\0';
                    break;
                }
                case 'K': {
                    char key_str[MAX_KEY_LEN] = {0};
                    if (!check_input_password(optarg)) {
                        do_advice();
                        exit(1);
                    }
                    GS_FREE(g_plainkey);
                    g_plainkey = xstrdup(optarg);
                    if (!mask_single_passwd(optarg)) {
                        (void)fprintf(
                            stderr, _("%s: mask passwd failed. optarg is null, or out of memory!\n"), progname);
                        do_advice();
                        exit(1);
                    }
                    nRet = snprintf_s(key_str, MAX_KEY_LEN, MAX_KEY_LEN - 1, "-K %s ", "***");
                    securec_check_ss_c(nRet, "\0", "\0");
                    rc = strncat_s(loginfo, MAXLOGINFOLEN, key_str, strlen(key_str));
                    securec_check_c(rc, "\0", "\0");

                    loginfo[MAXLOGINFOLEN - 1] = '\0';
                    break;
                }
                case 'S': {
                    char key_str[MAX_KEY_LEN] = {0};
                    GS_FREE(g_cipherkey);
                    g_cipherkey = xstrdup(optarg);
                    if (!mask_single_passwd(optarg)) {
                        (void)fprintf(
                            stderr, _("%s: mask passwd failed. optarg is null, or out of memory!\n"), progname);
                        do_advice();
                        exit(1);
                    }
                    nRet = snprintf_s(key_str, MAX_KEY_LEN, MAX_KEY_LEN - 1, "-S %s ", "***");
                    securec_check_ss_c(nRet, "\0", "\0");
                    rc = strncat_s(loginfo, MAXLOGINFOLEN, key_str, strlen(key_str));
                    securec_check_c(rc, "\0", "\0");
                    loginfo[MAXLOGINFOLEN - 1] = '\0';
                    break;
                }
                case 0:
                    /* This covers the long options. */
                    break;

                case 1: /* lcname */
                    GS_FREE(g_lcname);
                    check_env_value_c(optarg);
                    g_lcname = xstrdup(optarg);
                    break;

                case 2: /* 2 is ignore-node */
                    clear_g_ignore_nodeInfo();
                    saveIgnoreNodeElems(optarg);
                    break;

                default:
                    do_advice();
                    exit(1);
            }
        }
        /* Process an action */
        if (optind < argc) {
            get_action_value(ctl_command, (const char*)argv[optind]);
            optind++;
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (!bhave_nodetype) {
        bhave_nodetype = true;
        nodetype = INSTANCE_DATANODE;
        node_type_value[node_type_number++] = INSTANCE_DATANODE;
    }
#endif

    if (ctl_command == NO_COMMAND) {
        write_stderr(_("%s: no operation specified\n"), progname);
        do_advice();
        exit(1);
    }
    char arguments[MAX_BUF_SIZE] = {0x00};
    for (int i = 0; i < argc; i++) {
        if ((strlen(arguments) + strlen(argv[i])) >= (MAX_BUF_SIZE - 2)) {
            if (*arguments) {
                (void)write_log("The gs_guc run with the following arguments: [%s].\n", arguments);
            }
            (void)write_log("The gs_guc run with the following arguments: [%s].\n", argv[i]);
            rc = memset_s(arguments, MAX_BUF_SIZE, 0, MAX_BUF_SIZE - 1);
            securec_check_c(rc, "\0", "\0");
            continue;
        }
        errno_t rc = strcat_s(arguments, MAX_BUF_SIZE, argv[i]);
        size_t len = strlen(arguments);
        if (rc != EOK) {
            break;
        }
        arguments[len] = ' ';
        arguments[len + 1] = '\0';
    }
    if (*arguments) {
        (void)write_log("The gs_guc run with the following arguments: [%s].\n", arguments);
    }

    check_encrypt_options();
    
    if (ctl_command == ENCRYPT_KEY_COMMAND) {
        process_encrypt_cmd(pgdata_D, pgdata_C, pgdata_R);
        (void)write_log("gs_guc encrypt %s\n", loginfo);
    } else if (ctl_command == GENERATE_KEY_COMMAND) {
        doGenerateOperation(pgdata_D, loginfo);
    }

    if (ctl_command == ENCRYPT_KEY_COMMAND || ctl_command == GENERATE_KEY_COMMAND) {
        GS_FREE(g_prefix);
        GS_FREE(g_plainkey);
        GS_FREE(g_cipherkey);
        GS_FREE(key_username);
        GS_FREE(pgdata_D);
        GS_FREE(pgdata_R);
        GS_FREE(pgdata_C);
        return 0;
    }

    if (false == allocate_memory_list()) {
        write_stderr(_("ERROR: Failed to allocate memory to list.\n"));
        exit(1);
    }

    if (ctl_command != ENCRYPT_KEY_COMMAND && ctl_command != GENERATE_KEY_COMMAND && (!bhave_param && !is_hba_conf)) {
        write_stderr(_("%s: the form of this command is incorrect\n"), progname);
        do_advice();
        exit(1);
    }

    if (ctl_command != ENCRYPT_KEY_COMMAND && ctl_command != GENERATE_KEY_COMMAND && !bhave_nodetype) {
        write_stderr(_("%s: the type of node is not specified\n"), progname);
        do_advice();
        exit(1);
    }

    if ((g_ignore_nodeInfo != NULL) &&
        (ctl_command != SET_CONF_COMMAND) &&
        (ctl_command != RELOAD_CONF_COMMAND)) {
        write_stderr(_("%s: --ignore-node must be used with set/reload operation\n"), progname);
        clear_g_ignore_nodeInfo();
        exit(1);
    }

    if ((g_ignore_nodeInfo != NULL) &&
        (nodetype != INSTANCE_DATANODE) &&
        (nodetype != INSTANCE_COORDINATOR)) {
        write_stderr(_("%s: --ignore-node must be used with DN/CN\n"), progname);
        clear_g_ignore_nodeInfo();
        exit(1);
    }

    if ((g_ignore_nodeInfo != NULL) &&
        (pgdata_D != NULL)) {
        write_stderr(_("%s:  -D and -ignore-node cannot be used together\n"), progname);
        clear_g_ignore_nodeInfo();
        exit(1);
    }

    /*
     * set/reload: cn, dn, cma, cms and gtm
     * check:  cn and dn
     */
    check_ctl_command(ctl_command, nodetype);

    if ((nodetype == INSTANCE_CMAGENT) || (nodetype == INSTANCE_CMSERVER)) {
        checkCMParameter(pgdata_D, nodename, instance_name);
    }

    if (nodetype == INSTANCE_DATANODE || nodetype == INSTANCE_COORDINATOR) {
        check_build_status(pgdata_D);
    }

    if (nodename == NULL &&
        instance_name == NULL &&
        pgdata_D == NULL &&
        ((nodetype == INSTANCE_CMAGENT) || (nodetype == INSTANCE_CMSERVER)))
    {
        nodename = xstrdup("all");
        instance_name = xstrdup("all");
    }

    // log output redirect
    init_log(PROG_NAME);

    if ((true == is_hba_conf) &&
        ((nodetype != INSTANCE_COORDINATOR) && (nodetype != INSTANCE_DATANODE))) {
        write_stderr(_("%s: authentication operation (-h) is not supported for \"gtm\" or \"gtm_proxy\"\n"), progname);
        do_advice();
        exit(1);
    }

    // the number of -Z is equal to 2
    if (node_type_number == LARGE_INSTANCE_NUM) {
        if (node_type_value[0] == node_type_value[1]) {
            (void)write_stderr("When the number of -Z is equal to 2, the value must be different.\n");
            exit(1);
        }

        for (int index = 0; index < LARGE_INSTANCE_NUM; index++) {
            if (node_type_value[index] != INSTANCE_COORDINATOR && node_type_value[index] != INSTANCE_DATANODE) {
                (void)write_stderr("ERROR: When the number of -Z is equal to 2, the parameter value of -Z must be "
                                   "coordinator or datanode.\n");
                exit(1);
            }
            checkLcName(node_type_value[index]);
        }
        nodetype = INSTANCE_COORDINATOR;

        if (0 != validate_cluster_guc_options(nodename, nodetype, instance_name, pgdata_D)) {
            exit(1);
        }
        process_cluster_guc_option(nodename, nodetype, instance_name, pgdata_D);
    } else {
        checkLcName(nodetype);
        if (0 != validate_cluster_guc_options(nodename, nodetype, instance_name, pgdata_D)) {
            exit(1);
        }
        process_cluster_guc_option(nodename, nodetype, instance_name, pgdata_D);
    }

    GS_FREE(pgdata_D);
    GS_FREE(instance_name);

    nRet = print_guc_result((const char*)nodename);
    GS_FREE(nodename);
    clear_g_incorrect_nodeInfo();
    clear_g_ignore_nodeInfo();
    if (is_hba_conf) {
        free_hba_params();
    }
    if (0 != g_remote_command_result) {
        return 1;
    }
    if (0 != nRet)
        return 1;
    else
        return 0;
}

static void checkCMParameter(const char *dataDir, const char *nodeName, const char *instName)
{
    int i = 0;

    /*
      * This branch is not support.
      *   1. -D cm_path
      *   2. -I xxx
      *   3. -I all
      *   4. -N xxx -I all
      *   5. -N all -I xxx
      */
    if (NULL != dataDir &&
        0 != strncmp(dataDir, "cm_instance_data_path",
                    strlen(dataDir) > strlen("cm_instance_data_path") ? strlen(dataDir) : strlen("cm_instance_data_path"))) {
        (void)write_stderr("ERROR: When the '-Z' parameter is cmserver or cmagent, the -D parameter must be NULL.\n");
        exit(1);
    } else if (instName != NULL && 0 != strncmp(instName, "all" , strlen("all"))) {
        (void)write_stderr("ERROR: When the '-Z' parameter is cmserver or cmagent, -N parameter and -I parameter must be all or NULL at the same time.\n");
        exit(1);
    } else if (instName != NULL && 0 == strncmp(instName, "all" , strlen("all")) && nodeName == NULL) {
        (void)write_stderr("ERROR: When the '-Z' parameter is cmserver or cmagent, -N parameter and -I parameter must be all or NULL at the same time.\n");
        exit(1);
    } else if (nodeName != NULL && instName != NULL
            && 0 != strncmp(instName, "all" , strlen("all")) && 0 == strncmp(nodeName, "all" , strlen("all"))) {
        (void)write_stderr("ERROR: When the '-Z' parameter is cmserver or cmagent, -N parameter and -I parameter must be all or NULL at the same time.\n");
        exit(1);
    } else if (nodeName != NULL && instName != NULL
            && 0 == strncmp(instName, "all" , strlen("all")) && 0 != strncmp(nodeName, "all" , strlen("all"))) {
        (void)write_stderr("ERROR: When the '-Z' parameter is cmserver or cmagent, -N parameter and -I parameter must be all or NULL at the same time.\n");
        exit(1);
    }

    for (i = 0; i < config_value_number; i++)
    {
        if (NULL == config_value[i]) {
            (void)write_stderr("ERROR: cmserver or cmagent does not support -c \"parameter\".\n");
            exit(1);
        }
    }
}

/*************************************************************************************
 Function: print_gucinfo
 Desc    : print the "gs_guc check" result.
 Input   : paraname  -    the parameter name
 Return  : void
 *************************************************************************************/
void print_gucinfo(const char* paraname)
{
    int32 i = 0;

    (void)write_stderr("The details for %s:\n", paraname);
    for (i = 0; i < (int32)g_real_gucInfo->paramname_num; i++) {
        /* only print the value of paraname */
        if (strcmp(g_real_gucInfo->paramname_array[i], paraname) == 0) {
            char* guc_value = g_real_gucInfo->paramvalue_array[i];
            int j = 0;
            while (guc_value[j] != '\0' && guc_value[j] != '\n' && guc_value[j] != '\r')
                j++;
            guc_value[j] = '\0';

            (void)write_stderr("    [%s]  %s=%s  [%s]\n",
                g_real_gucInfo->nodename_array[i],
                g_real_gucInfo->paramname_array[i],
                g_real_gucInfo->paramvalue_array[i],
                g_real_gucInfo->gucinfo_array[i]);
        }
    }
}
/*************************************************************************************
 Function: find_first_paravalue_index
 Desc    : get the index of the parameter value which paraname we got from
               g_real_gucInfo->paramname_num  firstly
 Input   : paraname  -    the parameter name
 Return  : int
           -1       It means the parameter name is not found in g_real_gucInfo->paramname_num.
                    This scene does not be occured.
           i        The index number
 *************************************************************************************/
int find_first_paravalue_index(const char* paraname)
{
    int32 i = 0;

    for (i = 0; i < (int32)g_real_gucInfo->paramname_num; i++) {
        if (strcmp(g_real_gucInfo->paramname_array[i], paraname) == 0)
            return i;
        else
            continue;
    }
    return -1;
}
/*************************************************************************************
 Function: find_same_paravalue_index
 Desc    : if the parameter values have "NULL" and the parameter values are not same, return -1;
           else, return the first parameter value index number
 Input   : paraname  -    the parameter name
 Return  : int
           -1       The parameter values have "NULL" and the parameter values are not same
            i       The index number
 *************************************************************************************/
int find_same_paravalue_index(const char* paraname)
{
    int32 i = 0;
    int32 index = -1;

    /* We can makesure that the index > -1. Because after doing ""gs_guc check", the paraname must be in
     * g_real_gucInfo->paramname_array*/
    index = find_first_paravalue_index(paraname);

    for (i = 0; i < (int32)g_real_gucInfo->paramname_num; i++) {
        if (strcmp(g_real_gucInfo->paramname_array[i], paraname) == 0) {
            if (strncmp(g_real_gucInfo->paramvalue_array[i], "NULL", strlen("NULL")) == 0)
                return -1;

            if (strcmp(g_real_gucInfo->paramvalue_array[index], g_real_gucInfo->paramvalue_array[i]) != 0)
                return -1;
        } else {
            continue;
        }
    }
    return index;
}
/*******************************************************************************
 Function    : print_check_result
 Description : print the "gs_guc check" result
 Input       : None
 Output      : None
 Return      : void
*******************************************************************************/
void print_check_result()
{
    int i = 0;
    int index = 0;

    for (i = 0; i < config_param_number; i++) {
        index = find_same_paravalue_index(config_param[i]);
        if (-1 != index) {
            (void)write_stderr("The value of parameter %s is same on all instances.\n", config_param[i]);

            int j;
            j = 0;
            char* guc_value = g_real_gucInfo->paramvalue_array[index];
            while (guc_value[j] != '\0' && guc_value[j] != '\n' && guc_value[j] != '\r')
                j++;
            guc_value[j] = '\0';

            (void)write_stderr("    %s=%s\n", config_param[i], g_real_gucInfo->paramvalue_array[index]);
        } else {
            print_gucinfo(config_param[i]);
        }
    }
    (void)write_stderr("\n");
}
/*******************************************************************************
 Function    : print_guc_result
 Description : The output of the set/reload/check statistics
 Input       : None
 Output      : None
 Return      : int
               0   - Success
               1   - Failure
*******************************************************************************/
int print_guc_result(const char* nodename)
{
    int* instance_status_array = NULL;
    int32 i = 0;
    int32 j = 0;

    /*
     * If the command is set/reload , -N all and -I all for GTM/CN/DN/CN&DN/CMA/CMS
     * there is no need to print message again.
     */
    if ((CHECK_CONF_COMMAND != ctl_command) &&
        nodename != NULL &&
        0 == strncmp(nodename, "all" , strlen("all"))) {
        (void)write_stderr("\n");
        return 0;
    }

    /* if g_remote_connection_signal is false, it means there have some nodes that failed to be connected */
    if (!g_remote_connection_signal || g_remote_command_result != 0) {
        (void)write_stderr("Failed node names:\n");
        for (i = 0; i < (int32)g_incorrect_nodeInfo->num; i++)
            (void)write_stderr("    [%s]\n", g_incorrect_nodeInfo->nodename_array[i]);
    } else {
        if (CHECK_CONF_COMMAND != ctl_command) {
            (void)write_stderr("\nTotal instances: %d. Failed instances: %d.\n",
                (int32)g_expect_gucInfo->gucinfo_num,
                (int32)(g_expect_gucInfo->gucinfo_num - g_real_gucInfo->gucinfo_num));
        } else {
            (void)write_stderr("\nTotal GUC values: %d. Failed GUC values: %d.\n",
                (int32)g_expect_gucInfo->gucinfo_num,
                (int32)(g_expect_gucInfo->gucinfo_num - g_real_gucInfo->gucinfo_num));
        }

        /* when g_expect_gucInfo->gucinfo_num is 0, it means there is no instance */
        if (0 == g_expect_gucInfo->gucinfo_num) {
            if (CHECK_CONF_COMMAND == ctl_command)
                (void)write_stderr("There is no instance.\n\n");
            else
                (void)write_stderr("Success to perform gs_guc!\n\n");
            return 0;
        }
    }

    if (g_expect_gucInfo->gucinfo_num > g_real_gucInfo->gucinfo_num) {
        if (CHECK_CONF_COMMAND == ctl_command)
            (void)write_stderr("Failed GUC values information:\n");
        else
            (void)write_stderr("Failed instance information:\n");

        instance_status_array = (int*)pg_malloc_zero(g_expect_gucInfo->gucinfo_num * sizeof(int));
        for (i = 0; i < (int32)g_expect_gucInfo->gucinfo_num; i++)
            instance_status_array[i] = 0;

        for (i = 0; i < (int32)g_expect_gucInfo->gucinfo_num; i++) {
            for (j = 0; j < (int32)g_expect_gucInfo->gucinfo_num; j++) {
                /* Don't use strncmp, may one is a subset of the other */
                if ((NULL != g_expect_gucInfo->nodename_array[i] && NULL != g_real_gucInfo->nodename_array[j]) &&
                    (NULL != g_expect_gucInfo->gucinfo_array[i] && NULL != g_real_gucInfo->gucinfo_array[j])) {
                    if ((0 == strcmp(g_expect_gucInfo->nodename_array[i], g_real_gucInfo->nodename_array[j])) &&
                        (0 == strcmp(g_expect_gucInfo->gucinfo_array[i], g_real_gucInfo->gucinfo_array[j])))
                        instance_status_array[i] = 1;
                }
            }
        }

        for (i = 0; i < (int32)g_expect_gucInfo->gucinfo_num; i++) {
            if (instance_status_array[i] == 0) {
                if (CHECK_CONF_COMMAND == ctl_command) {
                    (void)write_stderr("    [%s]  %s  [%s]\n",
                        g_expect_gucInfo->nodename_array[i],
                        g_expect_gucInfo->paramname_array[i],
                        g_expect_gucInfo->gucinfo_array[i]);
                } else {
                    (void)write_stderr(
                        "    [%s]    [%s]\n", g_expect_gucInfo->nodename_array[i], g_expect_gucInfo->gucinfo_array[i]);
                }
            }
        }

        (void)write_stderr("\n");
        if (CHECK_CONF_COMMAND != ctl_command)
            (void)write_stderr("Failure to perform gs_guc!\n\n");
        GS_FREE(instance_status_array);
        return 1;
    } else {
        if (g_remote_connection_signal && g_remote_command_result == 0) {
            if (CHECK_CONF_COMMAND == ctl_command)
                print_check_result();
            else
                (void)write_stderr("Success to perform gs_guc!\n\n");
            return 0;
        } else {
            if (CHECK_CONF_COMMAND != ctl_command)
                (void)write_stderr("Failure to perform gs_guc!\n\n");
            return 1;
        }
    }
}

char* get_ctl_command_type()
{
    switch (ctl_command) {
        case SET_CONF_COMMAND:
            return "set";
        case RELOAD_CONF_COMMAND:
            return "reload";
        case CHECK_CONF_COMMAND:
            return "check";
        default:
            break;
    }

    return "";
}
/*******************************************************************************
 Function    : get_instance_configfile
 Description : get instance configration file by instance data directory
 Input       : datadir  - instance data directory
 Output      :
 Return      : void
******************************************************************************/
void get_instance_configfile(const char* datadir)
{
    int nRet = 0;

    switch (nodetype) {
        case INSTANCE_COORDINATOR:
        case INSTANCE_DATANODE: {
            if (is_hba_conf) {
                nRet = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(gucconf_file, MAXPGPATH, MAXPGPATH - 1, "%s/pg_hba.conf", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(tempguc_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir, TEMP_PGHBACONF_FILE);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(gucconf_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s/pg_hba.conf.lock", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
            } else {
                nRet = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(gucconf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(tempguc_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir, TEMP_PGCONF_FILE);
                securec_check_ss_c(nRet, "\0", "\0");
                nRet = snprintf_s(gucconf_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf.lock", datadir);
                securec_check_ss_c(nRet, "\0", "\0");
            }
            break;
        }
        case INSTANCE_CMAGENT:
        {
            nRet = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_agent/cm_agent.pid", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_agent/cm_agent.conf", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(tempguc_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_agent/%s", datadir, TEMP_CMAGENTCONF_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_lock_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_agent/cm_agent.conf.lock", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            break;
        }
        case INSTANCE_CMSERVER:
        {
            nRet = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_server/cm_server.pid", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_server/cm_server.conf", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(tempguc_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_server/%s", datadir, TEMP_CMSERVERCONF_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_lock_file, MAXPGPATH, MAXPGPATH-1, "%s/cm_server/cm_server.conf.lock", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            break;
        }
        case INSTANCE_GTM: {
            nRet = snprintf_s(pid_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm.pid", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm.conf", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(tempguc_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir, TEMP_GTMCONF_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            nRet = snprintf_s(gucconf_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm.conf.lock", datadir);
            securec_check_ss_c(nRet, "\0", "\0");
            break;
        }
        default:
            break;
    }
}

/*******************************************************************************
 Function    : do_guc_set_reload_for_each_parameter
 Description : do gs_guc set/reload
 Input       : action_type - action type. set/reload
 Output      : None
 Return      : None
******************************************************************************/
void do_guc_set_reload_for_each_parameter(const char* action_type, const char* data_dir)
{
    if (!is_hba_conf) {
        if (SUCCESS != do_gucset(action_type, data_dir)) {
            return ;
        }
    } else {
        if (SUCCESS != do_hba_set(action_type))
            return ;
    }

    if (0 == strncmp(action_type, "reload", sizeof("reload")))
    {
        if (SUCCESS != do_config_reload())
            return ;
    }

    g_real_gucInfo->nodename_array[g_real_gucInfo->nodename_num++] = xstrdup(g_local_node_name);
    g_real_gucInfo->gucinfo_array[g_real_gucInfo->gucinfo_num++] = xstrdup(gucconf_file);

}
/*******************************************************************************
 Function    : do_guc_check_for_each_parameter
 Description : do gs_guc check
 Input       : action_type - action type: check
 Output      : None
 Return      : None
******************************************************************************/
void do_guc_check_for_each_parameter(const char* action_type)
{
    int i = 0;
    char* guc_value = NULL;

    for (i = 0; i < config_param_number; i++) {
        guc_value = do_guccheck(config_param[i]);
        if (NULL == guc_value) {
            return;
        } else if (strncmp(guc_value, "NoFound", strlen("NoFound")) == 0) {
            (void)write_stderr(
                "gs_guc %s: %s: %s=NULL: [%s]\n", action_type, g_local_node_name, config_param[i], gucconf_file);
            g_real_gucInfo->paramvalue_array[g_real_gucInfo->paramvalue_num++] = xstrdup("NULL");
        } else {
            int j;
            j = 0;
            while (guc_value[j] != '\0' && guc_value[j] != '\n' && guc_value[j] != '\r')
                j++;
            guc_value[j] = '\0';

            (void)write_stderr("gs_guc %s: %s: %s=%s: [%s]\n",
                action_type,
                g_local_node_name,
                config_param[i],
                guc_value,
                gucconf_file);
            g_real_gucInfo->paramvalue_array[g_real_gucInfo->paramvalue_num++] = xstrdup(guc_value);
        }

        g_real_gucInfo->nodename_array[g_real_gucInfo->nodename_num++] = xstrdup(g_local_node_name);
        g_real_gucInfo->gucinfo_array[g_real_gucInfo->gucinfo_num++] = xstrdup(gucconf_file);
        g_real_gucInfo->paramname_array[g_real_gucInfo->paramname_num++] = xstrdup(config_param[i]);

        GS_FREE(guc_value);
    }
}

/*******************************************************************************
 Function    : process_guc_command
 Description : do gs_guc set/reload
 Input       : datadir  - instance data directory
 Output      :
 Return      : int
               0 - success
******************************************************************************/
int process_guc_command(const char* datadir)
{
    if (NULL == datadir || '\0' == datadir[0])
        return 1;

    get_instance_configfile(datadir);

    switch (ctl_command) {
        case SET_CONF_COMMAND:
            do_guc_set_reload_for_each_parameter("set", datadir);
            break;
        case RELOAD_CONF_COMMAND:
            do_guc_set_reload_for_each_parameter("reload", datadir);
            break;
        case CHECK_CONF_COMMAND:
            do_guc_check_for_each_parameter("check");
            break;
        default:
            break;
    }

    return 0;
}

/*
 * isMatchOptionName - Check wether the option name is in the configure file.
 *
 * Params:
 * @optLine: Line in the configure file.
 * @paraName: Paramater name.
 * @paraLength: Paramater length.
 * @paraOffset: Paramater offset int the optLine.
 * @valueLength: Value length.
 * @valueOffset: Value offset int the optLine.
 *
 * Returns:
 * True, iff the option name is in the configure file; else false.
 */
static bool isMatchOptionName(
    char* optLine, const char* paraName, int paraLength, int* paraOffset, int* valueLength, int* valueOffset)
{
    char* p = NULL;
    char* q = NULL;
    char* tmp = NULL;

    p = optLine;

    /* Skip all the blanks at the begin of the optLine */
    p = skipspace(p);

    if ('#' == *p) {
        p++;
    }

    /* Skip all the blanks after '#' and before the paraName */
    p = skipspace(p);

    if (find_param_in_string(p, paraName, paraLength) != 0) {
        return false;
    }

    if (NULL != paraOffset) {
        *paraOffset = p - optLine;
    }
    p += paraLength;

    p = skipspace(p);

    /* If not '=', this optLine's format is wrong in configure file */
    if (*p != '=') {
        return false;
    }

    p++;

    /* Skip all the blanks after '=' and before the value */
    p = skipspace(p);

    if (strlen(p) != 0) {
        q = p + 1;
        tmp = q;
        while (*q && !('\n' == *q || '#' == *q)) {
            if (!isspace((unsigned char)*q)) {
                /* End of string */
                if ('\'' == *q) {
                    tmp = ++q;
                    break;
                } else {
                    tmp = ++q;
                }
            } else {
                /*
                 * If paraName is a string, the ' ' is considered to
                 * be part of the string.
                 */
                ('\'' == *p) ? tmp = ++q : q++;
            }
        }
    }

    if (NULL != valueOffset) {
        *valueOffset = p - optLine;
    }

    if (NULL != valueLength) {
        *valueLength = (NULL == tmp) ? 0 : (tmp - p);
    }

    return true;
}

/*
 * isOptLineCommented - Check wether the option line is commented with '#'.
 *
 * Params:
 * @optLine: Line in the configure file.
 *
 * Returns:
 * True, iff the option line starts with '#'; else false.
 */
bool isOptLineCommented(const char* optLine)
{
    char* tmp = NULL;

    if (NULL == optLine) {
        return false;
    }

    tmp = (char*)optLine;

    /* Skip all the blanks at the begin of the optLine */
    while (isspace((unsigned char)*tmp)) {
        tmp++;
    }

    if ('#' == *tmp) {
        return true;
    }

    return false;
}

/*
 * trimBlanksTwoEnds - Clear out all the blanks at the begin and end of string.
 *
 * Params:
 * @strPtr: A pointer to the string.
 *
 * Returns:
 * void, and the strPtr will be the output.
 */
void trimBlanksTwoEnds(char** strPtr)
{
    int strLen = 0;
    char* tmpPtr = NULL;

    if (NULL == strPtr) {
        (void)write_stderr(_("strPtr should not be NULL.\n"));
        return;
    }

    strLen = strnlen(*strPtr, MAX_PARAM_LEN);
    tmpPtr = *strPtr + strLen - 1;

    /* Trim all the blanks at the begin of the string */
    while (isspace((unsigned char)**strPtr)) {
        (*strPtr)++;
    }

    /* Trim all the blanks at the end of the string */
    while (isspace((unsigned char)*tmpPtr)) {
        *tmpPtr = '\0';
        tmpPtr--;
    }

    return;
}

static void check_key_mode(const char* mode)
{
    int slen = strlen("server");
    int clen = strlen("client");
    int srclen = strlen("source");
    if (NULL == mode || '\0' == mode[0]) /*never happen in normal case*/
    {
        (void)write_stderr(_("%s: invalid key mode,please check it\n"), progname);
        do_advice();
        exit(1);
    }
    if (0 == strncmp(mode, "server", slen) && '\0' == mode[slen]) {
        key_mode = SERVER_MODE;
    } else if (0 == strncmp(mode, "client", clen) && '\0' == mode[clen]) {
        key_mode = CLIENT_MODE;
    } else if (0 == strncmp(mode, "source", srclen) && '\0' == mode[srclen]) {
        key_mode = SOURCE_MODE;
    } else {
        (void)write_stderr(_("%s: the options of -M is not recognized\n"), progname);
        do_advice();
        exit(1);
    }
}

/* check whether the encryption options is valid*/
static void check_encrypt_options(void)
{
    if (ENCRYPT_KEY_COMMAND != ctl_command) {
        return;
    }
    if (g_plainkey == NULL) {
        g_plainkey = simple_prompt("Password: ", MAX_KEY_LEN + 1, false);
        if (!check_input_password(g_plainkey)) {
            write_stderr(_("%s: The input key must be %d~%d bytes and "
                "contain at least three kinds of characters!\n"),
                progname, MIN_KEY_LEN, MAX_KEY_LEN);
            do_advice();
            exit(1);
        }
    }
    if (key_mode == SERVER_MODE && key_username != NULL) {
        (void)write_stderr(
            _("%s: In server mode,the encrypt operation will ignore the specified user:%s\n"), progname, key_username);
    }
    if (key_mode == SOURCE_MODE && key_username != NULL) {
        (void)write_stderr(
            _("%s: In source mode,the encrypt operation will ignore the specified user:%s\n"), progname, key_username);
    }
}

