/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 * 
 * scanEreport.cpp
 *     Defines the entry point for the console application.
 *     compile raise to scanEreport.exe and run the program under make stage.
 *     The program will have many function:
 *     1. Scan all the *.cpp and *.l file from given directory;
 *     2. Parse the context which include call ereport or elog function;
 *     3. Compare the base error message info list with new scan error report;
 *     4. Auto generated errmsg.h file which used to find error number report to client and
 *        we can find cause and action according to the error number.
 *
 * IDENTIFICATION
 *        src/bin/gsqlerr/scanEreport.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <time.h>
#include "securec.h"
#include "securec_check.h"
/******************************* declare macro **********************************/
#define TARGET_EXTENTION_NAME_CPP "cpp"
#define TARGET_EXTENTION_NAME_L "l"
#define TARGET_EXTENTION_NAME_Y "y"
#define ERRMSG_FORMAT_OUTPUT_FILENAME "errmsg.txt"
#define ERRMSG_NEW_FILENAME "errmsg_new.txt"
#define ERRMSG_OLD_FILENAME "errmsg_old.txt"
#define ERRMSG_COMP_RESULT_FILENAME "errmsg_result.txt"
#define ERRMSG_FORMAT_OUTPUT_FILENAME_EXT "errmsg_distinct.txt"
#define ERRMSG_LOG_FILENAME "errmsg.log"
#define ERRMSG_FORMAT_OUTPUT_FILENAME_DISTRIBUTE "distribute_errmsg.txt"
#define ERRMSG_NEW_FILENAME_DISTRIBUTE "distribute_errmsg_new.txt"
#define ERRMSG_OLD_FILENAME_DISTRIBUTE "distribute_errmsg_old.txt"
#define ERRMSG_COMP_RESULT_FILENAME_DISTRIBUTE "distribute_errmsg_result.txt"
#define ERRMSG_FORMAT_OUTPUT_FILENAME_EXT_DISTRIBUTE "distribute_errmsg_distinct.txt"
#define ERRMSG_LOG_FILENAME_DISTRIBUTE "distribute_errmsg.log"
#define EREPORT_SCAN_NAME "ereport scan"
#define DISTRIBUTE_EREPORT_SCAN_NAME "distribute ereport scan"
#define STATE_DIR_TYPE ((int)(1))
#define STATE_FILE_TYPE ((int)(2))
#define STATE_OTHER_TYPE ((int)(0))

#define ERRMSG_MAX_NUM ((int)(20 * 1024))
#define FILE_NAME_MAX_LEN 256
#define MAXPATH 1024
#define STRING_MAX_LEN 512
#define ERROR_LOCATION_NUM 128

#define ERPARA_TYPE_ERRCODE "errcode"
#define ERPARA_TYPE_ERRMSG "errmsg"
#define ERPARA_TYPE_ERRMODULE "errmodule"
#define ERPARA_TYPE_ERRMSG_INTERNAL "errmsg_internal"
#define ERPARA_TYPE_ERRDETAIL "errdetail"
#define ERPARA_TYPE_ERRDETAILLOG "errdetail_log"
#define ERPARA_TYPE_ERRHINT "errhint"
#define ERPARA_TYPE_ERRCAUSE "errcause"
#define ERPARA_TYPE_ERRACTION "erraction"

#define ERRPROC_FUNC_EREPORT ((int)(1))
#define ERRPROC_FUNC_ELOG ((int)(2))

#define OP_TYPE_EXIST ((int)(0))
#define OP_TYPE_INSERT ((int)(1))
#define OP_TYPE_DELETE ((int)(2))
#define OP_TYPE_UPDATE ((int)(3))
#define OP_TYPE_INCOMPLETE ((int)(4))

#define MSG_NO_ERRMODULE ((int)(1))
#define MSG_NO_ERRCODE ((int)(2))
#define MSG_NO_ERRDETAIL ((int)(4))
#define MSG_NO_ERRCAUSE ((int)(8))
#define MSG_NO_ERRACTION ((int)(16))

#define GAUSSDB_HEADERFILE_FLAG ((int)(1))
#define GSQLERR_HEADERFILE_FLAG ((int)(2))
#define ENABLE_PRINT_FILE_NAME ((int)(1))
#define INVALID_ERROR_CAUSE "\"invalid\""
#define INVALID_ERROR_ACTION "\"invalid\""
#define IS_IMCOMPLETE_MESSAGE(errMsgItem) \
    (errMsgItem->cSqlState == NULL || errMsgItem->stErrmsg.cause == NULL || \
    errMsgItem->stErrmsg.action == NULL || errMsgItem->stErrmsg.cause == NULL || \
    strcmp(errMsgItem->stErrmsg.action, INVALID_ERROR_ACTION) == 0 || \
    strcmp(errMsgItem->stErrmsg.cause, INVALID_ERROR_CAUSE) ==0)
#define IS_EMPTY_STRING(str) \
    (str == NULL || str[0] == '\0')
#define ENABLE_SCAN 0
#define IS_EQUAL_STR(str1, str2) \
    (strcmp(str1, str2) == 0)
#define IS_ERROR_MESSAGE(errLevel) \
    (errLevel != NULL && (strcmp(errLevel, "ERROR") == 0 || \
    strcmp(errLevel, "FATAL") == 0 || strcmp(errLevel, "PANIC") == 0))
/******************************* type define **************************/
typedef struct ERPARA {
    char EpLevel[STRING_MAX_LEN];
    char EpCode[STRING_MAX_LEN];
    char EpMsg[STRING_MAX_LEN];
    char EpInteralMsg[STRING_MAX_LEN];
    char EpDetail[STRING_MAX_LEN];
    char EpDetailLog[STRING_MAX_LEN];
    char EpHint[STRING_MAX_LEN];
    char EpCause[STRING_MAX_LEN];
    char EpAction[STRING_MAX_LEN];
} ERPARA_INFO_T;

/* the location of generate error message */
typedef struct {
    char szFileName[FILE_NAME_MAX_LEN]; /* __FILE__ of ereport() call */
    unsigned int ulLineno;              /* __LINE__ of ereport() call */
} mppdb_err_msg_location_t;

/* define the detail error message  */
typedef struct {
    char msg[STRING_MAX_LEN];    /* description of the error code */
    char cause[STRING_MAX_LEN];  /* cause cases of the error code */
    char action[STRING_MAX_LEN]; /* next action for the error code */
} mppdb_detail_errmsg_t;

/* the struct of error message management */
typedef struct {
    int ulSqlErrcode;  /* mppdb error code */
    char cSqlState[5]; /* sqlstate error code */
    int mppdb_err_msg_locnum;
    mppdb_err_msg_location_t *astErrLocate[ERROR_LOCATION_NUM]; /* location of the error cause */
    mppdb_detail_errmsg_t stErrmsg;                            /* detail error message of the error code */
    char ucOpFlag;
} mppdb_err_msg_t;
int checkErrMsgIncomplete(const char* parent, const char *input_file_path);
/******************************* delare and init global variable *************************/
/* read line from file to buffer */
static char g_acBuf[16384] = {0};
static char g_acBuf1[16384] = {0};
static char acTmpBuf[16384] = {0};

char* g_ErrCodesFile = NULL;
char* g_ScanFile = NULL;
static char* g_sCurDir = NULL;
static char* g_sProgDir = NULL;
static FILE* logfile;
long g_lErrMsgFileLineNum = 0;
unsigned long g_ulErrMsgNum = 0;
unsigned long g_ulErrMsgOldNum = 0;
unsigned long g_ulErrMsgReadOldNum = 0;

mppdb_err_msg_t* g_stErrMsg = NULL;
mppdb_err_msg_t* g_stErrMsgOld = NULL;

static char* g_scGaussdbHdfile = (char*)"errmsg.h";
static char* g_scGsqlerrHdfile = (char*)"gsqlerr_errmsg.h";
int g_Check_ErrMsgCompare = 0;
int g_Check_ErrMsgStandard = 0;
/******************************* declare function prototype ****************************/
extern void getProgdir(const char* argv0);
extern int getStatType(char* fileName);
extern int scanDir(const char* parent, const char* input_file_path, const char* input_path_check);
extern int fileprocess(char* dir, char* szFileName, int check_flag);
extern int parseEpPara(char* szLine, ERPARA_INFO_T* pstOutErPara, int funcflag, int check_flag);
extern int parseEreport(char* szLine, ERPARA_INFO_T* pstOutErPara, int check_flag);
extern int parseElog(char* szLine, ERPARA_INFO_T* pstOutErPara, int check_flag);
extern char* getSqlStateByMacro(char* ErrnoMacro);
extern int saveErrMsg(char* errmsg, char* dir, char* scanfile, int lineno);
extern int readErrmsgFile();
int errmsg_format_output_ext();
extern int create_header_files(int iFileType);
extern int getErrmsgResult();
extern int compareErrmsg();
extern int outputCompResult();
void releaseMem(void);
int outputLog(FILE* logfile, bool isCloseFd, const char* format, ...) __attribute__((format(printf, 3, 4)));
int outputLogAndScreen(FILE* logfile, bool isCloseFd, const char* format, ...) __attribute__((format(printf, 3, 4)));
static char* input_path_check = NULL;
static char* parent = NULL;
static char* input_path = NULL;
static char errmsg_format_output_filename[FILE_NAME_MAX_LEN];
static char errmsg_new_filename[FILE_NAME_MAX_LEN];
static char errmsg_old_filename[FILE_NAME_MAX_LEN];
static char errmsg_comp_results_filename[FILE_NAME_MAX_LEN];
static char errmsg_format_output_filename_ext[FILE_NAME_MAX_LEN];
static char errmsg_log_filename[FILE_NAME_MAX_LEN];
static char app_name[FILE_NAME_MAX_LEN];
void parseOutputParam(int argc,  char* argv[]);
int checkErrMsgItem(const char* errmodule, ERPARA_INFO_T* pstOutErPara);
int checkDebugMsgItem(const char* errmodule);
static void check_env_value(const char* input_env_value);
/******************************* realize function ************************************/
int main(int argc, char* argv[])
{
    char* filerealpath_ptr = NULL;
    char fileresolved_name[PATH_MAX] = {0};
    char* dirrealpath_ptr = NULL;
    char dirresolved_name[PATH_MAX] = {0};
    char cwd[MAXPATH] = {0};
    int lRet = 0;
    char logfilename[MAXPATH] = {0};
    time_t timer;
    struct tm* pstTmInfo = NULL;
    char acStartTime[25] = {0};
    int rc = -1;
    int rc_check = -1;

    if (4 > argc) {
        return outputLog(NULL, false, "argc error.\n");
    }

    if ((NULL == argv) || (NULL == argv[1]) || (NULL == argv[2]) || (NULL == argv[4])) {
        return outputLog(NULL, false, "argv[1] or argv[2] or argv[4] is null.\n");
    }
    parseOutputParam(argc, argv);
    /* get program directory */
    getProgdir(argv[0]);
    if (NULL == g_sProgDir) {
        return outputLog(NULL, false, "call getProgdir get program directory error, argv[0]: %s. \n", argv[0]);
    }
    dirrealpath_ptr = realpath(g_sProgDir, dirresolved_name);
    if (NULL == dirrealpath_ptr) {
        printf("call resolved_path:%s get resolved_name error.\n", g_sProgDir);
        free(g_sProgDir);
        return -1;
    }

    if (NULL != g_sProgDir) {
        free(g_sProgDir);
        g_sProgDir = NULL;
    }

    g_sProgDir = dirrealpath_ptr;
    check_env_value(g_sProgDir);

    /* get current work directory */
    g_sCurDir = getcwd(cwd, MAXPATH);
    if (g_sCurDir == NULL) {
        return outputLog(NULL, false, "Get current dir fail.\n");
    }

    /* create the log file  */
    rc = sprintf_s(logfilename, sizeof(logfilename), "%s/%s", g_sCurDir, errmsg_log_filename);
    securec_check_ss_c(rc, "\0", "\0");
    logfile = fopen(logfilename, "w");
    if (logfile == NULL) {
        return outputLog(NULL, false, "could not open file \"%s\" for writing: %s\n", logfilename, strerror(errno));
    }

    /* statistic the current timer */
    (void)time(&timer);
    pstTmInfo = localtime(&timer);
    if (NULL != pstTmInfo)
        (void)strftime(acStartTime, 25, "%Y-%m-%d %H:%M:%S", pstTmInfo);

    fprintf(logfile, "--start time: %s  \n", acStartTime);

    /* get errorcodes dir */
    g_ErrCodesFile = argv[3];
    filerealpath_ptr = realpath(g_ErrCodesFile, fileresolved_name);
    if (NULL == filerealpath_ptr) {
        return outputLog(NULL, false, "call resolved_path:%s get resolved_name error.\n", g_ErrCodesFile);
    }
    g_ErrCodesFile = filerealpath_ptr;

    /* init malloc */
    g_stErrMsg = (mppdb_err_msg_t*)malloc(sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM);
    if (NULL == g_stErrMsg) {
        return outputLog(logfile, true, "malloc g_stErrMsg fail.\n");
    }
    rc = memset_s(g_stErrMsg, sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM, 0, sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM);
    securec_check_c(rc, "\0", "\0");

    g_stErrMsgOld = (mppdb_err_msg_t*)malloc(sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM);
    if (NULL == g_stErrMsgOld) {
        releaseMem();
        return outputLog(logfile, true, "malloc g_stErrMsgOld fail.\n");
    }
    rc = memset_s(g_stErrMsgOld, sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM, 0, sizeof(mppdb_err_msg_t) * ERRMSG_MAX_NUM);
    securec_check_c(rc, "\0", "\0");

    /* read template error message from errmsg.txt */
    lRet = readErrmsgFile();
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call readErrmsgFile fail.\n");
    }

    /* get scan dir */
    parent = realpath(argv[1], NULL);
    if (NULL == parent) {
        return outputLog(NULL, false, "call resolved_path:%s get resolved_name error.\n", argv[1]);
    }
    
    char tmp_str[MAXPATH];
    rc = sprintf_s(tmp_str, sizeof(tmp_str), "%s/%s", argv[1], argv[2]);
    securec_check_ss_c(rc, "\0", "\0");
    input_path = realpath(tmp_str, NULL);
    if (NULL == input_path) {
        return outputLog(NULL, false, "call resolved_path:%s/%s get resolved_name error.\n", argv[1], argv[2]);
    }

    char tmp_str_check[MAXPATH];
    rc_check = sprintf_s(tmp_str_check, sizeof(tmp_str_check), "%s/%s", argv[1], argv[4]);
    securec_check_ss_c(rc_check, "\0", "\0");
    input_path_check = realpath(tmp_str_check, NULL);
    if (NULL == input_path_check) {
        return outputLog(NULL, false, "call resolved_path:%s/%s get resolved_name error.\n", argv[1], argv[4]);
    }
    /* scan all files from root recursive */
    lRet = scanDir(parent, input_path, input_path_check);
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call scanDir scan file:%s fail.\n", input_path);
    }

    /* compare template error message info with current error scan from given directory  */
    lRet = compareErrmsg();
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call compareErrmsg fail.\n");
    }
    /* output the compare result */
    lRet = outputCompResult();
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call outputCompResult fail.\n");
    }

    lRet = errmsg_format_output_ext();
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call errmsg_format_output_ext fail.\n");
    }

    /* create header file according to the compare result supply to gaussdb link */
    lRet = create_header_files(GAUSSDB_HEADERFILE_FLAG);
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call create_header_files:%s fail.\n", g_scGaussdbHdfile);
    }

    /* create header file according to the compare result supply to gsqlerr link */
    lRet = create_header_files(GSQLERR_HEADERFILE_FLAG);
    if (0 != lRet) {
        releaseMem();
        return outputLog(logfile, true, "call create_header_files:%s fail.\n", g_scGsqlerrHdfile);
    }

    /* output statistic of errmsg result using for make check */
    lRet = getErrmsgResult();
    if (0 != lRet) {
        releaseMem();
        return outputLogAndScreen(logfile, true, "call getErrmsgResult fail.\n");
    }
    if (g_Check_ErrMsgStandard) {
        (void)outputLogAndScreen(logfile, false, "%s detect unstandarded message in code file!\n", app_name);
    }
    if ((g_Check_ErrMsgStandard != 0) && ENABLE_SCAN) {
        releaseMem();
        return outputLogAndScreen(logfile, true, "%s has failed!\n", app_name);
    }
    outputLogAndScreen(logfile, false, "%s has finished!\n", app_name);
    fclose(logfile);
    releaseMem();
    return 0;
}
void parseOutputParam(int argc,  char* argv[]) {
    int rc = 0;
    if (argv[5] == NULL) {
        rc = sprintf_s(errmsg_format_output_filename, sizeof(errmsg_format_output_filename), "%s", ERRMSG_FORMAT_OUTPUT_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_new_filename, sizeof(errmsg_new_filename), "%s", ERRMSG_NEW_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_old_filename, sizeof(errmsg_old_filename), "%s", ERRMSG_OLD_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_comp_results_filename, sizeof(errmsg_comp_results_filename), "%s", ERRMSG_COMP_RESULT_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_format_output_filename_ext, sizeof(errmsg_format_output_filename_ext), "%s", ERRMSG_FORMAT_OUTPUT_FILENAME_EXT);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_log_filename, sizeof(errmsg_log_filename), "%s", ERRMSG_LOG_FILENAME);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(app_name, sizeof(app_name), "%s", EREPORT_SCAN_NAME);
        securec_check_ss_c(rc, "\0", "\0");
    } else {
        rc = sprintf_s(errmsg_format_output_filename, sizeof(errmsg_format_output_filename), "./../../../../%s", ERRMSG_FORMAT_OUTPUT_FILENAME_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_new_filename, sizeof(errmsg_new_filename), "%s", ERRMSG_NEW_FILENAME_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_old_filename, sizeof(errmsg_old_filename), "%s", ERRMSG_OLD_FILENAME_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_comp_results_filename, sizeof(errmsg_comp_results_filename), "%s", ERRMSG_COMP_RESULT_FILENAME_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_format_output_filename_ext, sizeof(errmsg_format_output_filename_ext), "%s", ERRMSG_FORMAT_OUTPUT_FILENAME_EXT_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(errmsg_log_filename, sizeof(errmsg_log_filename), "%s", ERRMSG_LOG_FILENAME_DISTRIBUTE);
        securec_check_ss_c(rc, "\0", "\0");
        rc = sprintf_s(app_name, sizeof(app_name), "%s", DISTRIBUTE_EREPORT_SCAN_NAME);
        securec_check_ss_c(rc, "\0", "\0");
    }
}
void releaseMem(void)
{
    if (NULL != g_stErrMsg) {
        for (int i = 0; i < ERRMSG_MAX_NUM; i++) {
            for (int j = 0; j < ERROR_LOCATION_NUM; j++) {
                if (g_stErrMsg[i].astErrLocate[j] != NULL) {
                    free(g_stErrMsg[i].astErrLocate[j]);
                    g_stErrMsg[i].astErrLocate[j] = NULL;
                }
            }
        }
        free(g_stErrMsg);
        g_stErrMsg = NULL;
    }

    if (NULL != g_stErrMsgOld) {
        for (int i = 0; i < ERRMSG_MAX_NUM; i++) {
            for (int j = 0; j < ERROR_LOCATION_NUM; j++) {
                if (g_stErrMsgOld[i].astErrLocate[j] != NULL) {
                    free(g_stErrMsgOld[i].astErrLocate[j]);
                    g_stErrMsgOld[i].astErrLocate[j] = NULL;
                }
            }
        }
        free(g_stErrMsgOld);
        g_stErrMsgOld = NULL;
    }
    if (input_path_check != NULL) {
        free(input_path_check);
        input_path_check = NULL;
    }
    if (parent != NULL) {
        free(parent);
        parent = NULL;
    }
    if (input_path != NULL) {
        free(input_path);
        input_path = NULL;
    }
    return;
}

static void check_env_value(const char* input_env_value)
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
        if (strstr(input_env_value, danger_character_list[i]) != NULL) {
            fprintf(
                stderr, "invalid token \"%s\" in input_env_value: (%s)\n", danger_character_list[i], input_env_value);
            exit(1);
        }
    }
}

int outputLog(FILE* logfd, bool isCloseFd, const char* format, ...)
{
#define TMELEN 32
#define MSGLEN 1024

    char msg[MSGLEN] = {'0'};
    va_list args;
    (void)va_start(args, format);
    int ss_rc = vsprintf_s(msg, MSGLEN, format, args);
    securec_check_ss_c(ss_rc, "\0", "\0");

    va_end(args);

    if (NULL != logfd) {
        fprintf(logfd, "%s", msg);
        if (true == isCloseFd)
            fclose(logfd);
    } else
        printf("%s", msg);

    return -1;
}

int outputLogAndScreen(FILE* logfd, bool isCloseFd, const char* format, ...) {
    char msg[MSGLEN] = {'0'};
    va_list args;
    (void)va_start(args, format);
    int ss_rc = vsprintf_s(msg, MSGLEN, format, args);
    securec_check_ss_c(ss_rc, "\0", "\0");
    va_end(args);
    if (NULL != logfd) {
        fprintf(logfd, "%s", msg);
        if (true == isCloseFd)
            fclose(logfd);
    }
    printf("%s", msg);
    return -1;

}
/*
 * Extracts the actual name of the program as called -
 * stripped of .exe suffix if any
 */
void getProgdir(const char* argv0)
{
    const char* nodir_name = NULL;
    const char* p = NULL;
    int progdirlen = 0;
    int rc = 0;

    if (NULL == argv0)
        return;

    for (p = argv0; *p; p++) {
        if (*p == '/')
            nodir_name = p;
    }

    if (NULL == nodir_name)
        return;

    progdirlen = nodir_name - argv0 + 1;
    g_sProgDir = (char*)malloc(progdirlen);
    if (NULL == g_sProgDir) {
        return;
    }
    rc = memset_s(g_sProgDir, progdirlen, 0, progdirlen);
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(g_sProgDir, progdirlen, argv0, progdirlen);
    securec_check_c(rc, "\0", "\0");
    g_sProgDir[progdirlen - 1] = '\0';

    return;
}

/* get file state type */
int getStatType(char* fileName)
{
    struct stat buf;
    int result;
    errno_t errorno = EOK;
    errorno = memset_s(&buf, sizeof(struct stat), 0, sizeof(struct stat));
    securec_check_c(errorno, "\0", "\0");
    result = stat(fileName, &buf);
    if (result < 0) {
        fprintf(logfile, "filename:%s stat error.\n", fileName);
    }
    if (S_ISDIR(buf.st_mode)) {
        return STATE_DIR_TYPE;
    } else if (S_ISREG(buf.st_mode)) {
        return STATE_FILE_TYPE;
    } else {
        return STATE_OTHER_TYPE;
    }
}


typedef struct {
    char *dir;
    char *file;
} FileInfo;

static FileInfo *repallocFileInfo(FileInfo *file_infos, const int new_num, const int old_num)
{
    if (new_num < 0 || (size_t)new_num >= ((0x3fffffff) / sizeof(FileInfo *))) {
        free(file_infos);
        return NULL;
    }
    FileInfo *tmps = (FileInfo *)malloc(sizeof(FileInfo) * new_num);
    if (tmps == NULL) {
        free(file_infos);
        return NULL;
    }
    for (int i = 0; i < old_num; i++) {
        tmps[i].file = file_infos[i].file;
        tmps[i].dir = file_infos[i].dir;
    }
    for (int i = old_num; i < new_num; i++) {
        tmps[i].file = NULL;
        tmps[i].dir = NULL;
    }
    free(file_infos);
    return tmps;
}

static bool copyPath(FileInfo *file_info, char *path, char *file_name)
{
    int file_len = strlen(file_name);
    int dir_len = strlen(path) - file_len - 1;
    if (file_len > MAXPATH || file_len < 0) {
        return false;
    }
    file_info->file = (char *)malloc(file_len + 1);
    if (file_info->file == NULL) {
        (void)outputLog(logfile, false, "malloc file_info of \"%s/%s\" fail\n", path, file_name);
        return false;
    }
    int rc = strcpy_s(file_info->file, file_len + 1, file_name);
    securec_check_c(rc, "\0", "\0");
    path[dir_len] = 0;
    if (dir_len > MAXPATH || dir_len < 0) {
        return false;
    }
    file_info->dir = (char *)malloc(dir_len + 1);
    if (file_info->dir == NULL) {
        (void)outputLog(logfile, false, "malloc file_info of \"%s/%s\" fail\n", path, file_name);
        return false;
    }
    rc = strcpy_s(file_info->dir, dir_len + 1, path);
    securec_check_c(rc, "\0", "\0");
    return true;
}

static bool isValidFileName(char *line, const size_t max_len)
{   
    size_t str_len = strlen(line);
    if (str_len >= max_len) {
        return false;
    }
    if (line[str_len - 1] != '\n') {
        (void)outputLog(logfile, false, "format of file \"%s\" is error.\n", line);
        return false;
    }
    line[str_len - 1] = 0;
    char *suffix = strrchr(line, '.');
    if (suffix == NULL) {
        (void)outputLog(logfile, false, "format of file \"%s\" is error.\n", line);
        return false;
    }
    /* filter *.cpp or *.l file */
    if ((strcmp(suffix + 1, TARGET_EXTENTION_NAME_CPP)) != 0 &&
        (strcmp(suffix + 1, TARGET_EXTENTION_NAME_L) != 0) &&
        (strcmp(suffix + 1, TARGET_EXTENTION_NAME_Y) != 0)) {
        return false;
    }
    return true;
}

static void freeFileInfos(FileInfo *file_infos, int size)
{
    for (int i = 0; i < size; i++) {
        if (file_infos[i].file != NULL) {
            free(file_infos[i].file);
        }
        if (file_infos[i].dir != NULL) {
            free(file_infos[i].dir);
        }
    }
    free(file_infos);
}

/**
 * get sorted sub files
 *
 */
static FileInfo *leadSortedScanFiles(const char* parent, const char *input_file_path, int *size)
{
    *size = 0;
    FILE *file = fopen(input_file_path, "r");
    if (file == NULL) {
        (void)outputLog(logfile, false, "could not open file \"%s\" for reading", input_file_path);
        return NULL;
    }
    int file_info_num = ERRMSG_MAX_NUM;
    FileInfo *file_infos = (FileInfo *)malloc(sizeof(FileInfo) * file_info_num);
    if (file_infos == NULL) {
        (void)fclose(file);
        (void)outputLog(logfile, true, "malloc file_info fail.\n");
        return NULL;
    }
    int rc = memset_s(file_infos, sizeof(FileInfo) * file_info_num, 0, sizeof(FileInfo) * file_info_num);
    securec_check_c(rc, "\0", "\0");
    int num = 0;
    char line[MAXPATH] = {0};
    /* read every line from *.cpp or *.l to the buffer and parse */
    while (fgets(line, sizeof(line), file) != NULL) {
        /* file name is valid or not */
        if (!isValidFileName(line, MAXPATH)) {
            continue;
        }
        char path[MAXPATH];
        int rc = sprintf_s(path, MAXPATH, "%s/%s", parent, line);
        securec_check_ss_c(rc, "\0", "\0");
        char *file_name = strrchr(path, '/');
        if (file_name == NULL) {
            (void)outputLog(logfile, false, "format of file \"%s\" is error.\n", line);
            continue;
        }
        file_name++;
        if (num >= file_info_num) {
            file_infos = repallocFileInfo(file_infos, file_info_num + ERRMSG_MAX_NUM, file_info_num);
            file_info_num = file_info_num + ERRMSG_MAX_NUM;
            if (file_infos == NULL) {
                (void)fclose(file);
                return NULL;
            }
        }
        FileInfo *file_info = file_infos + num;
        num++;
        if (!copyPath(file_info, path, file_name)) {
            freeFileInfos(file_info, num);
            (void)fclose(file);
            return NULL;
        }
    }
    *size = num;
    (void)fclose(file);
    return file_infos;
}

void releaseFileInfos(FileInfo *file_infos, int size)
{
    for (int i = 0; i < size; i++) {
        FileInfo *file_info = file_infos + i;
        char *file_name = file_info->file;
        char *dir = file_info->dir;
        free(file_name);
        free(dir);
    }
    free(file_infos);
}

/* scan directory recursive, find *.cpp and *.l file */
int scanDir(const char* parent, const char* input_file_path, const char* input_path_check)
{
    int size = 0;
    int size_check = 0;
    FileInfo *file_infos = leadSortedScanFiles(parent, input_file_path, &size);
    if (file_infos == NULL) {
        return -1;
    }
    FileInfo *file_check_infos = leadSortedScanFiles(parent, input_path_check, &size_check);
    if (file_check_infos == NULL) {
        releaseFileInfos(file_infos, size);
        return -1;
    }
    /* scan sub directory recursive */
    for (int i = 0; i < size; i++) {
        int check_flag = 1;
        FileInfo * file_info = file_infos + i;
        char *file_name = file_info->file;
        char *dir = file_info->dir;
        /* entry current dir */
        (void)chdir(dir);
        char path[MAXPATH] = {0};
        int rc = sprintf_s(path, sizeof(path), "%s/%s", dir, file_name);
        securec_check_ss_c(rc, "\0", "\0");
        if (STATE_FILE_TYPE != getStatType(path)) {
            continue;
        }
        for (int j = 0; j < size_check; j++) {
            FileInfo *file_check_info = file_check_infos + j;
            char *file_check_name = file_check_info->file;
            char *dir_check = file_check_info->dir;
            char path_check[MAXPATH] = {0};
            int rc_check = sprintf_s(path_check, sizeof(path_check), "%s/%s", dir_check, file_check_name);
            securec_check_ss_c(rc_check, "\0", "\0");
            if (STATE_FILE_TYPE != getStatType(path_check)) {
                continue;
            }
            if (0 == strcmp(path, path_check)) {
                check_flag = 0;
                break;
            }
        }
        /* open file/parse file */
        if (0 == fileprocess(dir, file_name, check_flag)) {
            (void)fprintf(logfile, "Scan and process file:%s/%s success.\n", dir, file_name);
        }
    }
    releaseFileInfos(file_infos, size);
    releaseFileInfos(file_check_infos, size_check);
    return 0;
}
void checkErrMsg(const char* fileName, int lineno, const ERPARA_INFO_T* errInfo, int checkFlag) {
    if (checkFlag == 0) {
        return;
    }
    if (checkFlag & MSG_NO_ERRMODULE) {
        outputLog(logfile, false, "The errmsg: %s has no errmodule file: %s lineo: %d\n", errInfo->EpMsg, fileName, lineno);
    }
    if (checkFlag & MSG_NO_ERRCODE) {
        outputLog(logfile, false, "The errmsg: %s has no errcode file: %s lineo: %d\n", errInfo->EpMsg, fileName, lineno);
    }
    if (checkFlag & MSG_NO_ERRDETAIL) {
        outputLog(logfile, false, "The errmsg: %s has no errdetail file: %s lineo: %d \n", errInfo->EpMsg, fileName, lineno);
    }
    if (checkFlag & MSG_NO_ERRCAUSE) {
        outputLog(logfile, false, "The errmsg: %s has no errcause file: %s lineo: %d \n", errInfo->EpMsg, fileName, lineno);
    }
    if (checkFlag & MSG_NO_ERRACTION) {
        outputLog(logfile, false, "The errmsg: %s has no erraction file: %s lineo: %d \n", errInfo->EpMsg, fileName, lineno);
    }

}
/* process function ereport or elog */
int fileprocess(char* dir, char* szFileName, int check_flag)
{
    char* szExtName = NULL;
    FILE* file = NULL;
    int lineno = 0;
    int len = 0;
    char* pTmp = NULL;
    ERPARA_INFO_T stErPara;
    int funcflag = 0;
    int fnmacroflag = 0;
    int tmplen = 0;
    errno_t rc = EOK;

    if ((NULL == dir) || (NULL == szFileName))
        return -1;

    /* file name is valid or not */
    szExtName = strrchr(szFileName, '.');
    if (szExtName == NULL) {
        return outputLog(logfile, false, "format of file \"%s\" is error.\n", szFileName);
    }

    /* filter *.cpp or *.l file */
    if ((0 != strcmp(szExtName + 1, TARGET_EXTENTION_NAME_CPP)) &&
        (0 != strcmp(szExtName + 1, TARGET_EXTENTION_NAME_L)) &&
        (strcmp(szExtName + 1, TARGET_EXTENTION_NAME_Y) != 0)) {
        return -1;
    }
    file = fopen(szFileName, "r");
    if (file == NULL) {
        return outputLog(logfile, false, "could not open file \"%s\" for reading", szFileName);
    }

    rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, sizeof(g_acBuf));
    securec_check_c(rc, "\0", "\0");

    /* read every line from *.cpp or *.l to the buffer and parse */
    while (fgets(g_acBuf, sizeof(g_acBuf), file) != NULL) {
        lineno++;
        /* if there has no ereport or elog function call, continue the next line */
        if ((NULL == strstr(g_acBuf, "ereport(")) && (NULL == strstr(g_acBuf, "elog("))) {
            rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, strlen(g_acBuf));
            securec_check_c(rc, "\0", "\0");
            continue;
        }

        funcflag = (NULL != strstr(g_acBuf, "ereport(")) ? (1) : (0);

        pTmp = g_acBuf;
        while ((isspace((unsigned char)*pTmp)) || ('\t' == *pTmp))
            pTmp++;

        if ((0 != memcmp(pTmp, "ereport", strlen("ereport"))) && (0 != memcmp(pTmp, "elog", strlen("elog")))) {
            rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, strlen(g_acBuf));
            securec_check_c(rc, "\0", "\0");
            continue;
        }

        tmplen = strlen(g_acBuf);
        pTmp = &g_acBuf[tmplen - 1];
        while ((isspace((unsigned char)*pTmp)) || ('\t' == *pTmp) || ('\n' == *pTmp))
            pTmp--;

        /* the end of ereport or elog function call */
        if (';' != *pTmp) {
            /* the ereport or elog is locate in macro define */
            if ('\\' == *pTmp)
                fnmacroflag = 1;

            /* the end of line */
            if ((pTmp = strrchr(g_acBuf, '\n')) != NULL) {
                rc = memcpy_s(acTmpBuf, sizeof(acTmpBuf), g_acBuf, pTmp - g_acBuf);
                securec_check_c(rc, "\0", "\0");
                len = len + (pTmp - g_acBuf);
            } else {
                rc = memcpy_s(&acTmpBuf[len], sizeof(acTmpBuf) - len, g_acBuf, strlen(g_acBuf));
                securec_check_c(rc, "\0", "\0");
                len = len + strlen(g_acBuf);
            }

            /* read the context of inner ereport(...) or elog(...) function */
            while (fgets(g_acBuf1, sizeof(g_acBuf1), file) != NULL) {
                lineno++;

                pTmp = g_acBuf1;
                while ((isspace((unsigned char)*pTmp)) || ('\t' == *pTmp))
                    pTmp++;

                /* the line is locate in comment start */
                if ('*' == *pTmp) {
                    rc = memset_s(g_acBuf1, sizeof(g_acBuf1), 0, strlen(g_acBuf1));
                    securec_check_c(rc, "\0", "\0");
                    continue;
                }

                if (('/' == *pTmp) && (('/' == *(pTmp + 1)) || ('*' == *(pTmp + 1)))) {
                    rc = memset_s(g_acBuf1, sizeof(g_acBuf1), 0, strlen(g_acBuf1));
                    securec_check_c(rc, "\0", "\0");
                    continue;
                }

                /* the end of line */
                pTmp = strrchr(g_acBuf1, '\n');
                if (pTmp != NULL) {
                    rc = memcpy_s(&acTmpBuf[len], sizeof(acTmpBuf) - len, g_acBuf1, pTmp - g_acBuf1);
                    securec_check_c(rc, "\0", "\0");
                    len = len + (pTmp - g_acBuf1);
                } else {
                    rc = memcpy_s(&acTmpBuf[len], sizeof(acTmpBuf) - len, g_acBuf1, sizeof(g_acBuf1) - len);
                    securec_check_c(rc, "\0", "\0");
                    len = len + strlen(g_acBuf1);
                }

                tmplen = strlen(g_acBuf1);
                pTmp = &g_acBuf1[tmplen - 1];
                /* find the character with end of line */
                while (((isspace((unsigned char)*pTmp)) || ('\t' == *pTmp) || ('\n' == *pTmp)) && pTmp > g_acBuf1)
                    pTmp--;

                /* if end of the ereport or elog function call then break the scan */
                if ((';' == *pTmp) || (fnmacroflag && ('\\' != *pTmp)))
                    break;

                rc = memset_s(g_acBuf1, sizeof(g_acBuf1), 0, strlen(g_acBuf1));
                securec_check_c(rc, "\0", "\0");
            }
        } else /* one line */
        {
            rc = memcpy_s(&acTmpBuf[len], sizeof(acTmpBuf) - len, g_acBuf, strlen(g_acBuf));
            securec_check_c(rc, "\0", "\0");
        }

        /* parse error msg in ereport or elog */
        int checkFlag = parseEpPara(acTmpBuf, &stErPara, funcflag, check_flag); 
        if (checkFlag != -1) {
            /* save new scan errmsg */
            (void)saveErrMsg((char*)&stErPara, dir, szFileName, lineno);
            checkErrMsg(szFileName, lineno, &stErPara, checkFlag);
        }

        len = 0;
        fnmacroflag = 0;
        rc = memset_s(&stErPara, sizeof(stErPara), 0, sizeof(stErPara));
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(acTmpBuf, sizeof(acTmpBuf), 0, strlen(acTmpBuf));
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, strlen(g_acBuf));
        securec_check_c(rc, "\0", "\0");
    }

    fclose(file);
    return 0;
}

/* merge mutiple line of errmsg */
int mergeMutiLine(char* szErrMsg)
{
    char* pStart = NULL;
    char* pEnd = NULL;
    size_t len = 0;
    errno_t rc = EOK;

    if (NULL == szErrMsg) {
        return 0;
    }

    /* search \"\t\t\t\t\t\t\", merge errmsg in different lines */
    pStart = strstr(szErrMsg, "\"\t");

    if ((pStart != NULL) || ((pStart = strstr(szErrMsg, "\"\r")) != NULL) ||
        ((pStart = strstr(szErrMsg, "\" \\")) != NULL)) {
        pEnd = strstr(pStart, "\t\"");
        if ((pEnd != NULL) || ((pEnd = strstr(pStart, " \"")) != NULL)) {
            len = strlen(szErrMsg) - (pEnd + 2 - &szErrMsg[0]);
            rc = memmove_s(pStart, strlen(pStart), pEnd + 2, len);
            securec_check_c(rc, "\0", "\0");
            rc = memset_s(pStart + len, strlen(pStart) - len, 0, strlen(szErrMsg) - (pStart + len - &szErrMsg[0]));
            securec_check_c(rc, "\0", "\0");
            return 1;
        }
    }

    return 0;
}

/* optimize mutiple block and other special characteriatic */
void optimize_mutiblock(char* pErrmsg)
{
    char* pStart = NULL;
    errno_t rc = EOK;

    if (NULL == pErrmsg)
        return;

    /* '\t' transform to ' ' */
    pStart = pErrmsg;
    while ((pStart = strstr(pStart, "\t")) != NULL) {
        *pStart = ' ';
    }

    /* decrease multi block to one block */
    pStart = pErrmsg;
    while ((pStart = strstr(pStart, "  ")) != NULL) {
        rc = memmove_s(pStart, strlen(pStart), pStart + strlen(" "), strlen(pStart + strlen(" ")) + 1);
        securec_check_c(rc, "\0", "\0");
    }

    /* '\"' transform to ''' */
    /* optimize string \"%s\", delete \" */
    pStart = pErrmsg;
    while ((pStart = strstr(pStart, "\\\"")) != NULL) {
        rc = memmove_s(pStart, strlen(pStart), pStart + 1, strlen(pStart + 1) + 1);
        securec_check_c(rc, "\0", "\0");
        *pStart = '\'';
    }

    pStart = pErrmsg;
    while ((pStart = strstr(pStart, "\" \"")) != NULL) {
        rc = memmove_s(pStart, strlen(pStart), pStart + strlen("\" \""), strlen(pStart + strlen("\" \"")) + 1);
        securec_check_c(rc, "\0", "\0");
    }

    pStart = pErrmsg;
    while ((pStart = strstr(pStart, "\"\"")) != NULL) {
        rc = memmove_s(pStart, strlen(pStart), pStart + 1, strlen(pStart + 1) + 1);
        securec_check_c(rc, "\0", "\0");
        *pStart = '\"';
    }

    return;
}

int parseContent(char* szLine, char *content, char * contentType) {
    errno_t rc = memset_s(content, STRING_MAX_LEN, 0, STRING_MAX_LEN);
    securec_check_c(rc, "\0", "\0");
    char* pStart = strstr(szLine, contentType);
    char* pEnd = NULL;
    int msglen = 0;
    /* only parse errmsg except errmsg_internel/errmsg_plural */
    if ((pStart != NULL) && (*(pStart + strlen(contentType) + 1) != '_')) {
        /* there has no string in errmsg */
        if (0 != memcmp(pStart + strlen(contentType), "(\"", strlen("(\""))) {
            return -1;
        }
        pStart = strchr(pStart, '(');
        if ((pStart != NULL) && ((pStart + 1) != NULL)) {
            /* optimize muti block */
            optimize_mutiblock(pStart);

            /* the errmsg has some parameter */
            pEnd = strstr(pStart, "\",");
            if (pEnd != NULL) {
                rc = memcpy_s(content, STRING_MAX_LEN, pStart + 1, pEnd - (pStart + 1) + 1);
                securec_check_c(rc, "\0", "\0");

                pStart = content;
                pEnd = strstr(pStart, "\")");
                if (pEnd != NULL) {
                    msglen = (pEnd - pStart) + 1;
                    rc = memset_s((void*)&content[msglen],
                        STRING_MAX_LEN - msglen,
                        0,
                        strlen(content) - (pEnd - pStart));
                    securec_check_c(rc, "\0", "\0");
                }
            } else if ((pEnd = strstr(pStart, "\")")) != NULL) /* the errmsg has no parameter */
            {
                rc = memset_s(content, STRING_MAX_LEN, 0, STRING_MAX_LEN);
                securec_check_c(rc, "\0", "\0");

                rc = memcpy_s(content, STRING_MAX_LEN, pStart + 1, pEnd - (pStart + 1) + 1);
                securec_check_c(rc, "\0", "\0");
            } else /* other case */
            {
                pEnd = strchr(pStart, ',');
                if (pEnd != NULL) {
                    if (*(pEnd - 1) == ')') {
                        rc = memcpy_s(
                            content, STRING_MAX_LEN, pStart + 1, pEnd - 1 - (pStart + 1));
                    } else {
                        rc =
                            memcpy_s(content, STRING_MAX_LEN, pStart + 1, pEnd - (pStart + 1));
                    }
                    securec_check_c(rc, "\0", "\0");
                } else {
                    pEnd = strchr(pStart, ')');
                    if (pEnd != NULL) {
                        rc =
                            memcpy_s(content, STRING_MAX_LEN, pStart + 1, pEnd - (pStart + 1));
                        securec_check_c(rc, "\0", "\0");
                    }
                }
            }

            /* if errmsg has mutipile line, merge mutipile line */
            while (mergeMutiLine(content)) {
            }
        }
    }
    return 0;
}

/* parse error msg in ereport */
int parseEreport(char* szLine, ERPARA_INFO_T* pstOutErPara, int check_flag)
{
    char* pStart = NULL;
    char* pEnd = NULL;
    char* pStart_module = NULL;
    char* pEnd_module = NULL;
    char errmodule_tmp[512] = {0};
    errno_t rc = EOK;
    int res = 0;
    if ((NULL == pstOutErPara) || (NULL == szLine))
        return -1;

    /* get ErrCode */
    rc = memset_s(pstOutErPara->EpCode, sizeof(pstOutErPara->EpCode), 0, sizeof(pstOutErPara->EpCode));
    securec_check_c(rc, "\0", "\0");
    pStart = strstr(szLine, ERPARA_TYPE_ERRCODE);
    if (pStart != NULL) {
        pStart = strchr(pStart, '(');
        if ((pStart != NULL) && ((pStart + 1) != NULL)) {
            pEnd = strchr(pStart, ')');
            if (pEnd != NULL) {
                rc = memcpy_s(pstOutErPara->EpCode, sizeof(pstOutErPara->EpCode), pStart + 1, pEnd - (pStart + 1));
                securec_check_c(rc, "\0", "\0");
            }
        }
    }

    /* get ErrModule */
    pStart_module = strstr(szLine, ERPARA_TYPE_ERRMODULE);
    if (pStart_module != NULL) {
        pStart_module = strchr(pStart_module, '(');
        if ((pStart_module != NULL) && ((pStart_module + 1) != NULL)) {
            pEnd_module = strchr(pStart_module, ')');
            if (pEnd_module != NULL) {
                rc = strncpy_s(errmodule_tmp, sizeof(errmodule_tmp), pStart_module + 1, pEnd_module - (pStart_module + 1));
                securec_check_c(rc, "\0", "\0");
            }
        }
    }

    /* parse error message */
    if (parseContent(szLine, pstOutErPara->EpMsg, ERPARA_TYPE_ERRMSG) < 0) {
        return -1;
    }

    /* parse error cause */
    (void)parseContent(szLine, pstOutErPara->EpCause, ERPARA_TYPE_ERRCAUSE);

    /* parse action cause */
    (void)parseContent(szLine, pstOutErPara->EpAction, ERPARA_TYPE_ERRACTION);

    if (check_flag) {
        if(IS_ERROR_MESSAGE(pstOutErPara->EpLevel)) {
            res = checkErrMsgItem(errmodule_tmp, pstOutErPara); 
        } else {
            res = checkDebugMsgItem(errmodule_tmp);
        }
    }

    return res;
}

int checkErrMsgItem(const char* errmodule, ERPARA_INFO_T* pstOutErPara)
{
    int res = 0;
    if (IS_EMPTY_STRING(errmodule) || strcmp(errmodule, "MOD_ALL") == 0) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRMODULE;
    }

    if (IS_EMPTY_STRING(pstOutErPara->EpCode)) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRCODE;
    }
    if (IS_EMPTY_STRING(pstOutErPara->EpDetail)) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRDETAIL;
    }
    if (IS_EMPTY_STRING(pstOutErPara->EpCause)) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRCAUSE;
    }
    if (IS_EMPTY_STRING(pstOutErPara->EpAction)) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRACTION;
    }
    return res;
}

int checkDebugMsgItem(const char* errmodule)
{
    int res = 0;
    if (IS_EMPTY_STRING(errmodule) || strcmp(errmodule, "MOD_ALL") == 0) {
        g_Check_ErrMsgStandard = 1;
        res |= MSG_NO_ERRMODULE;
    }   
    return res;
}

void InitElogStruct(ERPARA_INFO_T* pstOutErPara)
{
    errno_t rc = EOK;

    /* elog has no errno, set ERRCODE_INTERNAL_ERROR */
    rc = memset_s(pstOutErPara->EpCode, sizeof(pstOutErPara->EpCode), 0, sizeof(pstOutErPara->EpCode));
    securec_check_c(rc, "\0", "\0");
    rc = strncpy_s(pstOutErPara->EpCode, STRING_MAX_LEN, "ERRCODE_INTERNAL_ERROR", STRING_MAX_LEN - 1);
    securec_check_c(rc, "\0", "\0");

    /* elog has no errcause, set INVALID_ERROR_CAUSE */
    rc = memset_s(pstOutErPara->EpCause, sizeof(pstOutErPara->EpCause), 0, sizeof(pstOutErPara->EpCause));
    securec_check_c(rc, "\0", "\0");
    rc = strncpy_s(pstOutErPara->EpCause, STRING_MAX_LEN, INVALID_ERROR_CAUSE, STRING_MAX_LEN - 1);
    securec_check_c(rc, "\0", "\0");

    /* elog has no erraction, set INVALID_ERROR_ACTION */
    rc = memset_s(pstOutErPara->EpAction, sizeof(pstOutErPara->EpAction), 0, sizeof(pstOutErPara->EpAction));
    securec_check_c(rc, "\0", "\0");
    rc = strncpy_s(pstOutErPara->EpAction, STRING_MAX_LEN, INVALID_ERROR_ACTION, STRING_MAX_LEN - 1);
    securec_check_c(rc, "\0", "\0");
}

/* parse error msg in elog */
int parseElog(char* szLine, ERPARA_INFO_T* pstOutErPara, int check_flag)
{
    char* pStart = NULL;
    char* pEnd = NULL;
    errno_t rc = EOK;
    int res = 0;
    if ((NULL == pstOutErPara) || (NULL == szLine))
        return -1;
    InitElogStruct(pstOutErPara);

    /* get ErrMsg */
    rc = memset_s(pstOutErPara->EpMsg, sizeof(pstOutErPara->EpMsg), 0, sizeof(pstOutErPara->EpMsg));
    securec_check_c(rc, "\0", "\0");

    /* errmsg start with '"' */
    pStart = strchr(szLine, '"');
    if ((pStart == NULL) || ((pStart + 1) == NULL)) {
        /* there has no string in errmsg */
        return -1;
    }

    /* optimize mutipile white space */
    optimize_mutiblock(pStart);

    /* errmsg end with quote '"' */
    pEnd = strchr(pStart + 1, '"');
    if (pEnd == NULL) {
        /* format of errmsg is wrong */
        return -1;
    }

    if (sizeof(pstOutErPara->EpMsg) < (unsigned int)(pEnd - pStart + 2))
        return -1;

    int ret = snprintf_s((char*)pstOutErPara->EpMsg, sizeof(pstOutErPara->EpMsg), pEnd - pStart + 1, "%s", pStart);
    if ((-1 == ret) && (pstOutErPara->EpMsg[0] == '\0'))
        return -1;

    /* if errmsg has mutipile line, merge mutipile line */
    while (mergeMutiLine(pstOutErPara->EpMsg)) {
    }
    return res;
}

/* parse the line in function ereport or elog */
int parseEpPara(char* szLine, ERPARA_INFO_T* pstOutErPara, int funcflag, int check_flag)
{
    char* pStart = NULL;
    char* pEnd = NULL;
    int ret = 0;
    int rc = 0;

    if ((NULL == szLine) || (NULL == pstOutErPara)) {
        return -1;
    }

    /* get error level */
    rc = memset_s(pstOutErPara->EpLevel, sizeof(pstOutErPara->EpLevel), 0, sizeof(pstOutErPara->EpLevel));
    securec_check_c(rc, "\0", "\0");
    pStart = strchr(szLine, '(');
    if ((pStart != NULL) && ((pStart + 1) != NULL)) {
        pEnd = strchr(pStart, ',');
        if (pEnd != NULL) {
            rc = memcpy_s(pstOutErPara->EpLevel, sizeof(pstOutErPara->EpLevel), pStart + 1, pEnd - (pStart + 1));
            securec_check_c(rc, "\0", "\0");
        } else {
            return -1;
        }
    } else {
        return -1;
    }

    /* parse ereport function context */
    if (ERRPROC_FUNC_EREPORT == funcflag) {
        ret = parseEreport(szLine, pstOutErPara, check_flag);
    } else /* parse elog function context */
    {
        ret = parseElog(szLine, pstOutErPara, check_flag);
    }

    return ret;
}

/* get sqlstate in errcodes.txt by sqlstate macro */
char* getSqlStateByMacro(char* ErrnoMacro)
{
    FILE* errcodesFile = NULL;
    static char acSqlState[5] = {0};
    char* pStr = NULL;
    errno_t rc = EOK;

    if (ErrnoMacro == NULL) {
        return NULL;
    }

    if (*ErrnoMacro == '\0') {
        return NULL;
    }

    /* open file errcodes.txt */
    errcodesFile = fopen(g_ErrCodesFile, "r");
    if (errcodesFile == NULL) {
        return NULL;
    }

    rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, sizeof(g_acBuf));
    securec_check_c(rc, "\0", "\0");
    /* find errno macro in the file errcodes.txt */
    while (fgets(g_acBuf, sizeof(g_acBuf), errcodesFile) != NULL) {
        if ((pStr = strstr(g_acBuf, ErrnoMacro)) != NULL) {
            if ((*(pStr + strlen(ErrnoMacro)) == ' ') || (*(pStr + strlen(ErrnoMacro)) == '\n')) {
                rc = memcpy_s(acSqlState, sizeof(acSqlState), g_acBuf, 5);
                securec_check_c(rc, "\0", "\0");
                fclose(errcodesFile);
                return acSqlState;
            }
        }
        rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, strlen(g_acBuf));
        securec_check_c(rc, "\0", "\0");
    }

    fprintf(logfile, "ErrnoMacro:%s is not exist in file:%s. \n", ErrnoMacro, g_ErrCodesFile);
    fclose(errcodesFile);
    return NULL;
}

/* save new scan errmsg in given directory */
int saveErrMsg(char* errmsg, char* dir, char* scanfile, int lineno)
{
    unsigned int i = 0;
    int j = 0;
    ERPARA_INFO_T* pstErPara = NULL;
    static int errnonum = 1;
    char* sqlstate = NULL;
    char csqlstate[1] = {0};
    mppdb_err_msg_t* pstErrMsgItem = NULL;
    errno_t rc = EOK;

    if ((errmsg == NULL) || (dir == NULL) || (scanfile == NULL)) {
        return outputLog(logfile, false, "errmsg or dir or scanfile is null. \n");
    }

    pstErPara = (ERPARA_INFO_T*)errmsg;

    /* error level filter */
    if ((0 != strcmp(pstErPara->EpLevel, "ERROR")) && (0 != strcmp(pstErPara->EpLevel, "FATAL")) &&
        (0 != strcmp(pstErPara->EpLevel, "PANIC"))) {
        return -1;
    }

    if (ERRMSG_MAX_NUM <= g_ulErrMsgNum) {
        return outputLog(logfile, false, "g_ulErrMsgNum is more than max num %d.\n", ERRMSG_MAX_NUM);
    }

    if (errnonum >= ERRMSG_MAX_NUM) {
        return outputLog(logfile, false, "errnonum is more than max num %d and errnonum is %d.\n", ERRMSG_MAX_NUM, errnonum);
    }

    /* get sqlstate in errcodes.txt by sqlstate macro */
    sqlstate = getSqlStateByMacro(pstErPara->EpCode);
    if (sqlstate == NULL) {
        sqlstate = csqlstate;
        *sqlstate = '\0';
    }
    /* save the error message item into g_stErrMsg */
    for (i = 0; i < g_ulErrMsgNum; i++) {
        pstErrMsgItem = &g_stErrMsg[i];

        /* the same error message report in many different files, save the location */
        if (memcmp(pstErPara->EpMsg, pstErrMsgItem->stErrmsg.msg, strlen(pstErPara->EpMsg)) == 0 &&
            IS_EQUAL_STR(pstErrMsgItem->stErrmsg.action, pstErPara->EpAction) &&
            IS_EQUAL_STR(pstErrMsgItem->stErrmsg.cause, pstErPara->EpCause)) {
            for (j = 0; j < pstErrMsgItem->mppdb_err_msg_locnum; j++) {
                if (0 == strcmp(pstErrMsgItem->astErrLocate[j]->szFileName, scanfile) &&
                    (pstErrMsgItem->astErrLocate[j]->ulLineno == (unsigned int)lineno)) {
                    return 0;
                }
            }

            if (ERROR_LOCATION_NUM >= pstErrMsgItem->mppdb_err_msg_locnum + 1) {
                int locNum = pstErrMsgItem->mppdb_err_msg_locnum;
                pstErrMsgItem->astErrLocate[locNum] =
                    (mppdb_err_msg_location_t*)malloc(sizeof(mppdb_err_msg_location_t));
                if (pstErrMsgItem->astErrLocate[locNum] == NULL) {
                    return outputLog(logfile, false, "Memory alloc failed for err locate\n");
                }

                rc = memset_s(pstErrMsgItem->astErrLocate[locNum], sizeof(mppdb_err_msg_location_t),
                    0, sizeof(mppdb_err_msg_location_t));
                securec_check_c(rc, "\0", "\0");
                rc = strcpy_s(pstErrMsgItem->astErrLocate[locNum]->szFileName,
                    FILE_NAME_MAX_LEN,
                    scanfile);
                securec_check_c(rc, "\0", "\0");
                pstErrMsgItem->astErrLocate[locNum]->ulLineno = lineno;
                pstErrMsgItem->mppdb_err_msg_locnum++;
            }

            errnonum++;
            return 0;
        }
    }

    /* Insert the new error item into g_stErrMsg */
    pstErrMsgItem = &g_stErrMsg[i];
    pstErrMsgItem->ulSqlErrcode = errnonum;
    if ('\0' != *sqlstate) {
        rc = memcpy_s(
            pstErrMsgItem->cSqlState, sizeof(pstErrMsgItem->cSqlState), sqlstate, sizeof(pstErrMsgItem->cSqlState));
        securec_check_c(rc, "\0", "\0");
    } else
        pstErrMsgItem->cSqlState[0] = '\0';

    pstErrMsgItem->astErrLocate[0] = (mppdb_err_msg_location_t*)malloc(sizeof(mppdb_err_msg_location_t));
    if (pstErrMsgItem->astErrLocate[0] == NULL) {
        return outputLog(logfile, false, "Memory alloc failed for err locate\n");
    }

    rc = memset_s(pstErrMsgItem->astErrLocate[0], sizeof(mppdb_err_msg_location_t),
        0, sizeof(mppdb_err_msg_location_t));
    securec_check_c(rc, "\0", "\0");
    rc = strcpy_s(pstErrMsgItem->astErrLocate[0]->szFileName, FILE_NAME_MAX_LEN, scanfile);
    securec_check_c(rc, "\0", "\0");
    pstErrMsgItem->astErrLocate[0]->ulLineno = lineno;
    pstErrMsgItem->mppdb_err_msg_locnum = 1;
    rc = memcpy_s(pstErrMsgItem->stErrmsg.msg, sizeof(pstErrMsgItem->stErrmsg.msg), pstErPara->EpMsg, STRING_MAX_LEN);
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(pstErrMsgItem->stErrmsg.cause, sizeof(pstErrMsgItem->stErrmsg.cause), pstErPara->EpCause, STRING_MAX_LEN);
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(pstErrMsgItem->stErrmsg.action, sizeof(pstErrMsgItem->stErrmsg.action), pstErPara->EpAction, STRING_MAX_LEN);
    securec_check_c(rc, "\0", "\0");
    pstErrMsgItem->ucOpFlag = OP_TYPE_INSERT;

    g_ulErrMsgNum++;
    errnonum++;

    return 0;
}

/* open and read errmsg.txt context to struct */
int readErrmsgFile()
{
    FILE* errcodesOldFile = NULL;
    char* pStart = NULL;
    char* pEnd = NULL;
    char acErrNo[6] = {0};
    mppdb_err_msg_t* pstErrMsgItem = NULL;
    int errmsgnum = 0;
    char bIsScanFlag[5] = {0};
    char acOutput[512] = {0};
    errno_t rc = EOK;
    int causeLineFlag = 0;
    int actionLineFlag = 0;

    rc = sprintf_s(acOutput, sizeof(acOutput), "%s/%s", g_sProgDir, errmsg_format_output_filename);
    securec_check_ss_c(rc, "\0", "\0");
    if (access(acOutput, F_OK)) {
        return outputLog(logfile,
            false,
            "FUNC:[%s] LINE[%d]: readErrmsgFile: accss file %s fail.\n",
            __func__,
            __LINE__,
            errmsg_format_output_filename);
    }

    rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
    securec_check_c(rc, "\0", "\0");
    rc = sprintf_s(acOutput,
        sizeof(acOutput),
        "cp %s/%s %s/%s",
        g_sProgDir,
        errmsg_format_output_filename,
        g_sProgDir,
        errmsg_old_filename);
    securec_check_ss_c(rc, "\0", "\0");
    /* backup errmsg.txt as old */
    int ret = system(acOutput);
    if (ret == -1 || WIFEXITED(ret) == 0) {
        return outputLog(
            logfile, false, "FUNC:[%s] LINE[%d]: system func execute %s fail.\n", __func__, __LINE__, acOutput);
    }

    rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
    securec_check_c(rc, "\0", "\0");
    rc = sprintf_s(acOutput, sizeof(acOutput), "%s/%s", g_sProgDir, errmsg_old_filename);
    securec_check_ss_c(rc, "\0", "\0");
    errcodesOldFile = fopen(acOutput, "r");
    if (errcodesOldFile == NULL) {
        return outputLog(
            logfile, false, "FUNC:[%s] LINE[%d]: fopen file %s fail.\n", __func__, __LINE__, errmsg_old_filename);
    }

    rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, sizeof(g_acBuf));
    securec_check_c(rc, "\0", "\0");
    pstErrMsgItem = &g_stErrMsgOld[0];
    /* Read every line context in file errmsg_old.txt
        find token and insert context into g_stErrMsgOld */
    while (fgets(g_acBuf, sizeof(g_acBuf), errcodesOldFile) != NULL) {
        if ((pStart = strstr(g_acBuf, "GAUSS-")) != NULL) {
            pStart = pStart + strlen("GAUSS-");
            if (strchr(pStart, ':') != NULL) {
                pEnd = strchr(pStart, ':');
            }

            if (NULL == pEnd) {
                fclose(errcodesOldFile);
                return -1;
            }

            if (5 >= pEnd - pStart) {
                rc = memcpy_s(acErrNo, sizeof(acErrNo), pStart, pEnd - pStart);
                securec_check_c(rc, "\0", "\0");
                pstErrMsgItem->ulSqlErrcode = atoi(acErrNo);
                rc = memset_s(acErrNo, sizeof(acErrNo), 0, 5);
                securec_check_c(rc, "\0", "\0");
            }

            pStart = pEnd + 2;
            if (pStart != NULL) {
                if ((pEnd = strrchr(pStart, '\n')) != NULL) {
                    *pEnd = '\0';
                }
                rc = strcpy_s(pstErrMsgItem->stErrmsg.msg, sizeof(pstErrMsgItem->stErrmsg.msg), pStart);
                securec_check_c(rc, "\0", "\0");
            }

            bIsScanFlag[1] = 1;
        }

        if ((pStart = strstr(g_acBuf, "SQLSTATE: ")) != NULL) {
            pStart = pStart + strlen("SQLSTATE: ");
            if (pStart != NULL) {
                if (*pStart == '\n') {
                    *(pstErrMsgItem->cSqlState) = '\0';
                } else {
                    rc = memcpy_s(pstErrMsgItem->cSqlState, sizeof(pstErrMsgItem->cSqlState), pStart, 5);
                    securec_check_c(rc, "\0", "\0");
                }
            }

            bIsScanFlag[2] = 1;
        }

        if ((pStart = strstr(g_acBuf, "CAUSE: ")) != NULL) {
            pStart = pStart + strlen("CAUSE: ");
            if (pStart != NULL) {
                if ((pEnd = strrchr(pStart, '\n')) != NULL) {
                    *pEnd = '\0';
                }
                rc = strcpy_s(pstErrMsgItem->stErrmsg.cause, sizeof(pstErrMsgItem->stErrmsg.cause), pStart);
                securec_check_c(rc, "\0", "\0");
            }
            bIsScanFlag[3] = 1;
        }

        if ((pStart = strstr(g_acBuf, "ACTION: ")) != NULL) {
            pStart = pStart + strlen("ACTION: ");
            if (pStart != NULL) {
                if ((pEnd = strrchr(pStart, '\n')) != NULL) {
                    *pEnd = '\0';
                }
                rc = strcpy_s(pstErrMsgItem->stErrmsg.action, sizeof(pstErrMsgItem->stErrmsg.action), pStart);
                securec_check_c(rc, "\0", "\0");
            }
            bIsScanFlag[4] = 1;
        }

        if ((bIsScanFlag[1] == 1) && (bIsScanFlag[2] == 1) && bIsScanFlag[3] == 1 && actionLineFlag == 1) {
            if (bIsScanFlag[4] == 0) {
                fclose(errcodesOldFile);
                return outputLog(logfile, false, "The errmsg : %s has no action\n", pstErrMsgItem->stErrmsg.msg); 
            }
            actionLineFlag = 0;
        }

        if ((bIsScanFlag[1] == 1) && (bIsScanFlag[2] == 1) && causeLineFlag == 1) {
            if (bIsScanFlag[3] == 0) {
                fclose(errcodesOldFile);
                return outputLog(logfile, false, "The errmsg : %s has no cause\n", pstErrMsgItem->stErrmsg.msg);
            }
            if (bIsScanFlag[3] == 1 && bIsScanFlag[4] == 0) {
                actionLineFlag = 1;
            }
            causeLineFlag = 0;
        }

        if ((bIsScanFlag[1] == 1) && (bIsScanFlag[2] == 1)) {
             if (bIsScanFlag[3] == 0) {
                causeLineFlag = 1;
             }
        }

        if ((bIsScanFlag[1] == 1) && (bIsScanFlag[2] == 1) && (bIsScanFlag[3] == 1) && (bIsScanFlag[4] == 1)) {
            errmsgnum++;
            if (ERRMSG_MAX_NUM <= errmsgnum) {
                break;
            }
            pstErrMsgItem = &g_stErrMsgOld[errmsgnum];
            rc = memset_s(bIsScanFlag, sizeof(bIsScanFlag), 0, 5);
            securec_check_c(rc, "\0", "\0");
        }

        rc = memset_s(g_acBuf, sizeof(g_acBuf), 0, strlen(g_acBuf));
        securec_check_c(rc, "\0", "\0");
    }

    fclose(errcodesOldFile);
    g_ulErrMsgOldNum = errmsgnum;
    g_ulErrMsgReadOldNum = errmsgnum;
    fprintf(logfile, "FUNC:[%s] LINE[%d]: process readErrmsgFile success.\n", __func__, __LINE__);
    return 0;
}

/* output one errmsg item */
void outputErrmsgItem(FILE* outFile, mppdb_err_msg_t* pstErrMsgItem, int printFileName = 0)
{
    if (NULL == pstErrMsgItem)
        return;

    fprintf(outFile, "\nGAUSS-%05d: %s\n", pstErrMsgItem->ulSqlErrcode, pstErrMsgItem->stErrmsg.msg);

    fprintf(outFile, "SQLSTATE: %s\n", pstErrMsgItem->cSqlState);

    if (0 == strlen(pstErrMsgItem->stErrmsg.cause))
        fprintf(outFile, "CAUSE: %s\n", INVALID_ERROR_CAUSE);
    else
        fprintf(outFile, "CAUSE: %s\n", pstErrMsgItem->stErrmsg.cause);

    if (0 == strlen(pstErrMsgItem->stErrmsg.action))
        fprintf(outFile, "ACTION: %s\n", INVALID_ERROR_ACTION);
    else
        fprintf(outFile, "ACTION: %s\n", pstErrMsgItem->stErrmsg.action);
    if (printFileName == ENABLE_PRINT_FILE_NAME) {
        fprintf(outFile, "\n\nFile: %s", pstErrMsgItem->astErrLocate[0]->szFileName);
    }
    fprintf(outFile, "\n");
    return;
}

/* format output errmsg */
int errmsg_format_output_ext()
{
    unsigned int i = 0;
    FILE* outFile = NULL;
    char acOutput[512] = {0};
    mppdb_err_msg_t* pstErrMsgItem = NULL;
    int rc = -1;

    rc = sprintf_s(acOutput, sizeof(acOutput), "%s/%s", g_sProgDir, errmsg_new_filename);
    securec_check_ss_c(rc, "\0", "\0");
    if (!access(acOutput, F_OK)) {
        /* rm ERRMSG_FORMAT_OUTPUT_FILENAME */
        rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
        securec_check_c(rc, "\0", "\0");
        rc = sprintf_s(acOutput, sizeof(acOutput), "rm %s/%s", g_sProgDir, errmsg_new_filename);
        securec_check_ss_c(rc, "\0", "\0");
        int ret = system(acOutput);
        if (ret == -1 || WIFEXITED(ret) == 0)
            return outputLog(
                logfile, false, "FUNC:[%s] LINE[%d]: system func execute %s fail.\n", __func__, __LINE__, acOutput);
    }

    rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
    securec_check_c(rc, "\0", "\0");
    rc = sprintf_s(acOutput, sizeof(acOutput), "%s/%s", g_sProgDir, errmsg_new_filename);
    securec_check_ss_c(rc, "\0", "\0");
    outFile = fopen(acOutput, "w+");
    if (outFile == NULL) {
        return -1;
    }

    if (ERRMSG_MAX_NUM <= g_ulErrMsgOldNum) {
        fclose(outFile);
        return outputLog(logfile, false, "g_ulErrMsgOldNum exceed max num:%d.\n", ERRMSG_MAX_NUM);
    }

    fprintf(outFile, "/* Autogenerated file, please don't edit */\n\n");

    for (i = 0; i < g_ulErrMsgOldNum; i++) {
        pstErrMsgItem = &g_stErrMsgOld[i];

        /* if the errmsg item opflag is DELETE, then can not output to errmsg.txt */
        if (OP_TYPE_DELETE == pstErrMsgItem->ucOpFlag) {
            continue;
        }
        outputErrmsgItem(outFile, pstErrMsgItem);
    }

    fclose(outFile);
    return 0;
}

int checkErrMsgIncomplete(const char* parent, const char *input_file_path) {
    int res = 0;
    int size = 0;
    FileInfo *file_infos = leadSortedScanFiles(parent, input_file_path, &size);
    for (int i = 0; i < (int)g_ulErrMsgOldNum; i++) {
        mppdb_err_msg_t* pstErrMsgItem = &g_stErrMsgOld[i];
        if (OP_TYPE_INCOMPLETE == (pstErrMsgItem->ucOpFlag & OP_TYPE_INCOMPLETE)){
            int checked = 1;
            for(int j = 0; j < size; j++) {
                for (int k = 0; k < ERROR_LOCATION_NUM && pstErrMsgItem->astErrLocate[k]->szFileName[0] != '\0'; k++) {
                    if (strcmp(file_infos[j].file, pstErrMsgItem->astErrLocate[k]->szFileName) == 0) {
                        checked = 0;
                        break;
                     }
                }
                if (checked == 0) {
                    break;
                }
            }
            if (checked) {
                res = -1;
                (void)outputLog(logfile, false, "fileName: %s incomplete message: %s\n", pstErrMsgItem->astErrLocate[0]->szFileName, pstErrMsgItem->stErrmsg.msg);
            }
        }
    }
    releaseFileInfos(file_infos, size);
    return res;

}

/* process compare errmsg between new errmsg and old errmsg,
     and record insert/update/delete/incomplete of errmsg */
int compareErrmsg()
{
    mppdb_err_msg_t* pstErrMsgItemNew = NULL;
    mppdb_err_msg_t* pstErrMsgItemOld = NULL;
    unsigned int lNewLoop = 0;
    unsigned int lOldLoop = 0;
    unsigned long ExistCount = 0;
    errno_t rc = EOK;

    if ((0 == g_ulErrMsgNum) || (0 == g_ulErrMsgOldNum)) {
        return outputLog(
            logfile, false, "g_ulErrMsgNum:%lu or g_ulErrMsgOldNum:%lu is 0.\n", g_ulErrMsgNum, g_ulErrMsgOldNum);
    }
    /* compare the old and new errmsg list item */
    for (lOldLoop = 0; lOldLoop < g_ulErrMsgOldNum; lOldLoop++) {
        pstErrMsgItemOld = &g_stErrMsgOld[lOldLoop];
        for (lNewLoop = 0; lNewLoop < g_ulErrMsgNum; lNewLoop++) {
            pstErrMsgItemNew = &g_stErrMsg[lNewLoop];

            /* there are same with errmsg and sqlstate between old and new errmsg list item */
            if ((memcmp(pstErrMsgItemOld->stErrmsg.msg,
                     pstErrMsgItemNew->stErrmsg.msg,
                     strlen(pstErrMsgItemOld->stErrmsg.msg)) == 0) &&
                 IS_EQUAL_STR(pstErrMsgItemOld->stErrmsg.action, pstErrMsgItemNew->stErrmsg.action) &&
                 IS_EQUAL_STR(pstErrMsgItemOld->stErrmsg.cause,  pstErrMsgItemNew->stErrmsg.cause))  {
                /*
                 * update location info of old errmsg using new errmsg.
                 * 1. if pstErrMsgItemOld->mppdb_err_msg_locnum >= pstErrMsgItemNew->mppdb_err_msg_locnum,
                 *    copy all new location to old one, there must be enough slot for copy.
                 * 2. else need to malloc memory for old msg item before copy.
                 */
                for (int i = pstErrMsgItemOld->mppdb_err_msg_locnum; i < pstErrMsgItemNew->mppdb_err_msg_locnum; i++) {
                    pstErrMsgItemOld->astErrLocate[i] =
                        (mppdb_err_msg_location_t*)malloc(sizeof(mppdb_err_msg_location_t));
                    if (pstErrMsgItemOld->astErrLocate[i] == NULL) {
                        return outputLog(logfile, false, "Memory alloc failed for err locate\n");
                    }
                }
                for (int i = 0; i < pstErrMsgItemNew->mppdb_err_msg_locnum; i++) {
                    rc = memcpy_s(pstErrMsgItemOld->astErrLocate[i], sizeof(mppdb_err_msg_location_t),
                        pstErrMsgItemNew->astErrLocate[i], sizeof(mppdb_err_msg_location_t));
                    securec_check_c(rc, "\0", "\0");
                }

                pstErrMsgItemOld->mppdb_err_msg_locnum = pstErrMsgItemNew->mppdb_err_msg_locnum;

                pstErrMsgItemNew->ucOpFlag = OP_TYPE_EXIST;
                ExistCount++;
                if (IS_IMCOMPLETE_MESSAGE(pstErrMsgItemOld)) {
                    pstErrMsgItemOld->ucOpFlag |= OP_TYPE_INCOMPLETE;
                }

                break;
            }
        }

        if (lNewLoop >= g_ulErrMsgNum) {
            /* delete errmsg item from g_stErrMsgOld in errmsg_format_output_ext */
            /* output to result for delete errmsg */
            pstErrMsgItemOld->ucOpFlag = OP_TYPE_DELETE;
        }
    }


    /* output and save the result of insert errmsg */
    for (lNewLoop = 0; lNewLoop < g_ulErrMsgNum; lNewLoop++) {
        pstErrMsgItemNew = &g_stErrMsg[lNewLoop];
        if (OP_TYPE_INSERT == pstErrMsgItemNew->ucOpFlag) {
            if (ERRMSG_MAX_NUM <= g_ulErrMsgOldNum + 1) {
                fprintf(logfile, "g_ulErrMsgOldNum has arrived max.\n");
                break;
            }

            pstErrMsgItemOld = &g_stErrMsgOld[g_ulErrMsgOldNum];
            rc = memcpy_s(pstErrMsgItemOld, sizeof(mppdb_err_msg_t), pstErrMsgItemNew, sizeof(mppdb_err_msg_t));
            securec_check_c(rc, "\0", "\0");

            /* for astErrLocate, we should malloc mem and copy one by one */
            for (int i = 0; i < pstErrMsgItemNew->mppdb_err_msg_locnum; i++) {
                pstErrMsgItemOld->astErrLocate[i] = (mppdb_err_msg_location_t*)malloc(sizeof(mppdb_err_msg_location_t));
                if (pstErrMsgItemOld->astErrLocate[i] == NULL) {
                    return outputLog(logfile, false, "Memory alloc failed for err locate\n");
                }

                rc = memcpy_s(pstErrMsgItemOld->astErrLocate[i], sizeof(mppdb_err_msg_location_t),
                    pstErrMsgItemNew->astErrLocate[i], sizeof(mppdb_err_msg_location_t));
                securec_check_c(rc, "\0", "\0");
            }

            pstErrMsgItemOld->ulSqlErrcode = g_stErrMsgOld[g_ulErrMsgOldNum - 1].ulSqlErrcode + 1;
            pstErrMsgItemOld->ucOpFlag = OP_TYPE_INSERT;
            if (IS_IMCOMPLETE_MESSAGE(pstErrMsgItemOld)) {
                pstErrMsgItemOld->ucOpFlag |= OP_TYPE_INCOMPLETE;
            }
            g_ulErrMsgOldNum++;
        }
    }

    return 0;
}

/* output compare result between new errmsg and old errmsg */
int outputCompResult()
{
    FILE* outFile = NULL;
    char acOutput[512] = {0};
    unsigned int ulLoop = 0;
    int ulInsertNum = 0;
    int ulDeleteNum = 0;
    int ulUpdateNum = 0;
    mppdb_err_msg_t* pstErrMsgItemOld = NULL;
    int rc = -1;

    rc = sprintf_s(acOutput, sizeof(acOutput), "%s/%s", g_sProgDir, errmsg_comp_results_filename);
    securec_check_ss_c(rc, "\0", "\0");
    outFile = fopen(acOutput, "w+");
    if (outFile == NULL) {
        return outputLog(logfile, false, "open file:%s fail.\n", acOutput);
    }

    fprintf(outFile, "/*****************INSERT ERRMSG***************/\n");
    for (ulLoop = 0; ulLoop < g_ulErrMsgOldNum; ulLoop++) {
        pstErrMsgItemOld = &g_stErrMsgOld[ulLoop];

        if (OP_TYPE_INSERT == (pstErrMsgItemOld->ucOpFlag & OP_TYPE_INSERT)) {
            outputErrmsgItem(outFile, pstErrMsgItemOld, ENABLE_PRINT_FILE_NAME);
            ulInsertNum++;
        }
    }

    fprintf(outFile, "\n/*****************DELETE ERRMSG***************/\n");
    for (ulLoop = 0; ulLoop < g_ulErrMsgOldNum; ulLoop++) {
        pstErrMsgItemOld = &g_stErrMsgOld[ulLoop];

        if (OP_TYPE_DELETE == (pstErrMsgItemOld->ucOpFlag & OP_TYPE_DELETE)) {
            outputErrmsgItem(outFile, pstErrMsgItemOld);
            ulDeleteNum++;
        }
    }

    fprintf(outFile, "\n/*****************IMCOMPLETE ERRMSG***************/\n");
    for (ulLoop = 0; ulLoop < g_ulErrMsgOldNum; ulLoop++) {
        pstErrMsgItemOld = &g_stErrMsgOld[ulLoop];

        if (OP_TYPE_INCOMPLETE == (pstErrMsgItemOld->ucOpFlag & OP_TYPE_INCOMPLETE)) {
            outputErrmsgItem(outFile, pstErrMsgItemOld, ENABLE_PRINT_FILE_NAME);
            ulUpdateNum++;
        }
    }

    fprintf(outFile, "\n/********************SUMMARY******************/\n");
    fprintf(outFile, "Insert ErrorMsg Num: %d\n", ulInsertNum);
    fprintf(outFile, "Delete ErrorMsg Num: %d\n", ulDeleteNum);
    fprintf(outFile, "Incomplete ErrorMsg Num: %d\n", ulUpdateNum);
    fprintf(outFile, "ErrorMsg Num before Make: %lu\n", g_ulErrMsgReadOldNum);
    fprintf(outFile, "ErrorMsg Num after  Make: %lu\n", g_ulErrMsgReadOldNum - ulDeleteNum + ulInsertNum);

    fclose(outFile);
    return 0;
}

/* auto create errmsg.h files */
int create_header_files(int iFileType)
{
    unsigned int ulLoop = 0;
    int ulLocLoop = 0;
    FILE* outFile = NULL;
    char acOutput[512] = {0};
    mppdb_err_msg_t* pstErrMsgItem = NULL;
    errno_t rc = EOK;

    if (GAUSSDB_HEADERFILE_FLAG == iFileType) {
        rc = snprintf_s(acOutput, sizeof(acOutput), sizeof(acOutput) - 1, "%s/%s", g_sProgDir, g_scGaussdbHdfile);
        securec_check_ss_c(rc, "\0", "\0");
        if (!access(acOutput, F_OK)) {
            fprintf(logfile, "file %s exist.\n", acOutput);
            rc = snprintf_s(acOutput,
                sizeof(acOutput),
                sizeof(acOutput) - 1,
                "mv %s/%s errmsg_bak.h",
                g_sProgDir,
                g_scGaussdbHdfile);
            securec_check_ss_c(rc, "\0", "\0");
            int ret = system(acOutput);
            if (ret == -1 || WIFEXITED(ret) == 0)
                return outputLog(
                    logfile, false, "FUNC:[%s] LINE[%d]: system func execute %s fail.\n", __func__, __LINE__, acOutput);
        }

        rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
        securec_check_c(rc, "\0", "\0");
        rc = snprintf_s(acOutput, sizeof(acOutput), sizeof(acOutput) - 1, "%s/%s", g_sProgDir, g_scGaussdbHdfile);
        securec_check_ss_c(rc, "\0", "\0");

        outFile = fopen(acOutput, "w+");
        if (outFile == NULL) {
            return -1;
        }
    } else {
        rc = snprintf_s(acOutput, sizeof(acOutput), sizeof(acOutput) - 1, "%s/%s", g_sProgDir, g_scGsqlerrHdfile);
        securec_check_ss_c(rc, "\0", "\0");
        if (!access(acOutput, F_OK)) {
            fprintf(logfile, "file %s exist.\n", acOutput);
            rc = snprintf_s(acOutput,
                sizeof(acOutput),
                sizeof(acOutput) - 1,
                "mv %s/%s errmsg_bak.h",
                g_sProgDir,
                g_scGsqlerrHdfile);
            securec_check_ss_c(rc, "\0", "\0");
            int ret = system(acOutput);
            if (ret == -1 || WIFEXITED(ret) == 0)
                return outputLog(
                    logfile, false, "FUNC:[%s] LINE[%d]: system func execute %s fail.\n", __func__, __LINE__, acOutput);
        }

        rc = memset_s(acOutput, sizeof(acOutput), 0, sizeof(acOutput));
        securec_check_c(rc, "\0", "\0");
        rc = snprintf_s(acOutput, sizeof(acOutput), sizeof(acOutput) - 1, "%s/%s", g_sProgDir, g_scGsqlerrHdfile);
        securec_check_ss_c(rc, "\0", "\0");
        outFile = fopen(acOutput, "w+");
        if (outFile == NULL) {
            return -1;
        }
    }

    fprintf(outFile, "/* Autogenerated file, please don't edit */\n\n");
    fprintf(outFile, "#include \"errmsg_st.h\"\n\n");

    if (GAUSSDB_HEADERFILE_FLAG == iFileType)
        fprintf(outFile, "static const mppdb_err_msg_t g_mppdb_errors[] =\n");
    else
        fprintf(outFile, "static const gsqlerr_err_msg_t g_gsqlerr_errors[] =\n");

    fprintf(outFile, "{\n");

    for (ulLoop = 0; ulLoop < g_ulErrMsgOldNum; ulLoop++) {
        pstErrMsgItem = &g_stErrMsgOld[ulLoop];

        if (OP_TYPE_DELETE == pstErrMsgItem->ucOpFlag) {
            continue;
        }
        fprintf(outFile,
            "	  {%d, \"%s\",\n",
            pstErrMsgItem->ulSqlErrcode,
            ((0 == strlen(pstErrMsgItem->cSqlState)) ? "XX000" : pstErrMsgItem->cSqlState));

        if (GAUSSDB_HEADERFILE_FLAG == iFileType) {
            fprintf(outFile, "	  { ");
            for (ulLocLoop = 0; ulLocLoop < pstErrMsgItem->mppdb_err_msg_locnum; ulLocLoop++) {
                fprintf(outFile,
                    "{\"%s\", %u}, ",
                    pstErrMsgItem->astErrLocate[ulLocLoop]->szFileName,
                    pstErrMsgItem->astErrLocate[ulLocLoop]->ulLineno);
                if (0 == (ulLocLoop + 1) % 3) {
                    fprintf(outFile, "\n	 ");
                }
            }
            fprintf(outFile, "},\n");
        }

        if (GSQLERR_HEADERFILE_FLAG == iFileType) {
            fprintf(outFile, "	  {");
            if (0 != strlen(pstErrMsgItem->stErrmsg.msg)) {
                fprintf(outFile, "%s,\n", pstErrMsgItem->stErrmsg.msg);
            } else {
                fprintf(outFile, "NULL,\n");
            }

            if (0 != strlen(pstErrMsgItem->stErrmsg.cause)) {
                fprintf(outFile, "	   %s,\n", pstErrMsgItem->stErrmsg.cause);
            } else {
                fprintf(outFile, "	   %s,\n", INVALID_ERROR_CAUSE);
            }

            if (0 != strlen(pstErrMsgItem->stErrmsg.action)) {
                fprintf(outFile, "	   %s}", pstErrMsgItem->stErrmsg.action);
            } else {
                fprintf(outFile, "	   %s}", INVALID_ERROR_ACTION);
            }
        }

        fprintf(outFile, "},\n\n");
    }

    fprintf(outFile, "};\n");
    fclose(outFile);
    return 0;
}

/* output statistic of errmsg result
     using for make check */
int getErrmsgResult()
{
    char line[1024];
    char srcfile[MAXPATH] = {0};
    char buf[MAXPATH * 4] = {0};
    FILE* fHandle = NULL;
    char* pcStart = NULL;
    char* pcEnd = NULL;
    int ulInsertNum = 0;
    int ulDeleteNum = 0;
    int ulUpdateNum = 0;
    int ulTotalErrmsgOld = 0;
    int ulTotalErrmsgNew = 0;
    int i;
    int ret = 0;

    ret = snprintf_s(srcfile, MAXPATH, MAXPATH - 1, "%s/%s", g_sProgDir, errmsg_comp_results_filename);
    securec_check_ss_c(ret, "\0", "\0");

    fHandle = fopen(srcfile, "r");
    if (fHandle == NULL) {
        return outputLog(logfile, false, "could not open file \"%s\"\n", srcfile);
    }

    ret = memset_s(line, sizeof(line), 0, sizeof(line));
    securec_check_c(ret, "\0", "\0");
    while (fgets(line, sizeof(line), fHandle) != NULL) {
        if ((pcStart = strstr(line, "Insert ErrorMsg Num: ")) != NULL) {
            pcStart = pcStart + strlen("Insert ErrorMsg Num: ");
            if ((pcEnd = strrchr(line, '\n')) != NULL) {
                *pcEnd = '\0';
                ulInsertNum = atoi(pcStart);
            }
        } else if ((pcStart = strstr(line, "Delete ErrorMsg Num: ")) != NULL) {
            pcStart = pcStart + strlen("Delete ErrorMsg Num: ");
            if ((pcEnd = strrchr(line, '\n')) != NULL) {
                *pcEnd = '\0';
                ulDeleteNum = atoi(pcStart);
            }
        } else if ((pcStart = strstr(line, "Incomplete ErrorMsg Num: ")) != NULL) {
            pcStart = pcStart + strlen("Incomplete ErrorMsg Num: ");
            if ((pcEnd = strrchr(line, '\n')) != NULL) {
                *pcEnd = '\0';
                ulUpdateNum = atoi(pcStart);
            }
        } else if ((pcStart = strstr(line, "ErrorMsg Num before Make: ")) != NULL) {
            pcStart = pcStart + strlen("ErrorMsg Num before Make: ");
            if ((pcEnd = strrchr(line, '\n')) != NULL) {
                *pcEnd = '\0';
                ulTotalErrmsgOld = atoi(pcStart);
            }
        } else if ((pcStart = strstr(line, "ErrorMsg Num after  Make: ")) != NULL) {
            pcStart = pcStart + strlen("ErrorMsg Num after	Make: ");
            if ((pcEnd = strrchr(line, '\n')) != NULL) {
                *pcEnd = '\0';
                ulTotalErrmsgNew = atoi(pcStart);
            }
        } else {
        }
        ret = memset_s(line, sizeof(line), 0, sizeof(line));
        securec_check_c(ret, "\0", "\0");
    }

    if (ulInsertNum == 0 && ulDeleteNum == 0 && ulUpdateNum == 0 && ulTotalErrmsgNew != 0)
        ret = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, " All %d errmsg have no chang. ", ulTotalErrmsgOld);
    else if ((ulDeleteNum != 0) && (ulInsertNum == 0) && (ulUpdateNum == 0))
        ret = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, " %d of %d errmsg delete. ", ulDeleteNum, ulTotalErrmsgOld);
    else if ((ulInsertNum != 0) && (ulDeleteNum == 0) && (ulUpdateNum == 0))
        ret = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, " %d of %d errmsg insert. ", ulInsertNum, ulTotalErrmsgOld);
    else if ((ulUpdateNum != 0) && (ulInsertNum == 0) && (ulDeleteNum == 0))
        ret = snprintf_s(
            buf, sizeof(buf), sizeof(buf) - 1, " %d of %d errmsg incomplete. ", ulUpdateNum, ulTotalErrmsgOld);
    else
        ret = snprintf_s(buf,
            sizeof(buf),
            sizeof(buf) - 1,
            " %d errmsg insert, %d errmsg delete, %d errmsg incomplete. \n"
            " %d errmsg before make check, %d errmsg after make check. ",
            ulInsertNum,
            ulDeleteNum,
            ulUpdateNum,
            ulTotalErrmsgOld,
            ulTotalErrmsgNew);

    securec_check_ss_c(ret, "\0", "\0");

    fprintf(logfile, "\n");
    for (i = strlen(buf); i > 0; i--) {
        fprintf(logfile, "=");
    }
    fprintf(logfile, "\n%s\n", buf);
    for (i = strlen(buf); i > 0; i--) {
        fprintf(logfile, "=");
    }

    fprintf(logfile, "\n\n");

    if (ulInsertNum != 0 || ulDeleteNum != 0 || ulUpdateNum != 0) {
        fprintf(logfile,
            "The change of the error message in current version can be view in the file \"%s/%s\". \n"
            "You should be check modify the error message in the file \"%s/%s\".\n\n",
            g_sProgDir,
            errmsg_comp_results_filename,
            g_sProgDir,
            errmsg_new_filename);
    }

    fclose(fHandle);
    return 0;
}
