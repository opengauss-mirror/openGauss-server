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
 *-------------------------------------------------------------------------
 *
 * kt_main.cpp 
 *      Entry of gs_ktool :
 *      1. parsing and checking commands read from the command line;
 *      2. invoking executor to process commands.
 * 
 * IDENTIFICATION
 *      src/bin/gs_ktool/kt_main.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include "kt_key_manage.h"
#include "kt_log_manage.h"
#include "kt_kmc_callback.h"

const char *KT_VERSION = "gs_ktool V1.0.0 || KMC V3.0.0.SPC005";
const int EXIT = 0;
const int NOT_EXIT = 1;
const int UNUSED = 0;
const int MIN_KSF_PASS_CHAR_TYPE = 3;

const char *g_cmk_manage_arg[] = {"-g", "-l", "-d", "-s", "-i", "-e", "-f", "-p"};
const char g_invalid_cmk_manage_arg[] = {0, 0, 0, 0, 0, 0, 0, 0};
const int CMK_ARG_CNT = 8;

typedef struct ArgValue {
    bool has_set_all;
    unsigned int cmk_id;
    unsigned int cmk_len;
    char ksf[MAX_REAL_PATH_LEN];
    char ksf_passwd[MAX_KSF_PASS_BUF_LEN];
} ArgValue;

typedef void (*ManageCmkHookFunc)(ArgValue *arg_value);

typedef struct CmkManageFunc {
    char arg_patten[CMK_ARG_CNT];
    ManageCmkHookFunc manage_cmk_func;
    struct CmkManageFunc *next;
} CmkManageFunc;

typedef struct CmkManageFuncList {
    size_t node_cnt;
    CmkManageFunc *first_node;
} CmkManageFuncList;

enum ArgType {
    ARG_G = 0,
    ARG_L,
    ARG_D,
    ARG_S,
    ARG_I,
    ARG_E,
    ARG_F,
    ARG_P,
};

enum InputType {
    I_KEY_ID = 0,
    I_KEY_LEN,
    I_KSF,
};

enum AdviseType {
    HELP = 0,
    INPUT_PASSWD,
};

enum PasswdCharType {
    NUM = 0,
    UPPER,
    LOWER,
    SYMBOL,
};

enum PasswdErrType {
    LEGAL = 0,
    INVALID_LEN,
    INVALID_CHAR,
    FEW_TYPES,
};

/* read command from terminal */
int main(int argc, char *argv[]);
static void print_kt_version(void);
static void print_advise(AdviseType advise_type);
static void print_help_info(void);

/* parse command */
static void parse_cmd(int argc, char *argv[], char *arg_patten, ArgValue *arg_value);
static void parse_rk_manage_cmd(int argc, char *argv[]);

/* check commnad */
static bool get_passwd(char *passwd, const size_t pwbuf_size);
static PasswdErrType check_passwd(char *ksf_passwd);
static void report_passwd_err(PasswdErrType passwd_err);
static void check_user_input(const InputType input_type, const char *user_input);

/* register commnad and process funcs */
static bool is_arg_patten_equal(const char base_list[CMK_ARG_CNT], const char new_list[CMK_ARG_CNT]);
static bool reg_cmk_manage_func(CmkManageFuncList *cmk_manage_func_list, CmkManageFunc *new_func_node);
static CmkManageFunc *create_cmk_manage_func(const char arg_patten[CMK_ARG_CNT], ManageCmkHookFunc manage_cmk_func);
static void destroy_cmk_manage_func_list(CmkManageFuncList *cmk_manage_func_list);
static void reg_all_cmk_manage_func(CmkManageFuncList *cmk_manage_func_list);

/* process command */
static void process_cmd(const char arg_patten[CMK_ARG_CNT], ArgValue *arg_value);
static void process_gen_cmk_cmd(ArgValue *arg_value);
static void process_del_cmk_cmd(ArgValue *arg_value);
static void process_select_cmk_cmd(ArgValue *arg_value);
static void process_import_cmk_cmd(ArgValue *arg_value);
static void process_export_cmk_cmd(ArgValue *arg_value);

/* entry to all commands */
int main(int argc, char *argv[])
{    
    /* cmk manage args: -g -l -d -s -i -e -f -p */
    char arg_patten[] = {0, 0, 0, 0, 0, 0, 0, 0}; 
    ArgValue arg_value = { 0 };

    if (!initialize_logmodule()) {
        exit(1);
    }
    
    if (argc <= 1) {
        print_help_info();
        exit(0);
    } else if (argc > 6) {
        insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "the number of parameters exceeds the upper limit");
        exit(1);
    }

    /* start parsing single CLI commands */   
    if (strcmp(argv[1], "-?") == 0) {
        print_help_info();
    } else if (strcmp(argv[1], "-R") == 0) {
        parse_rk_manage_cmd(argc, argv);
    } else {
        /* unrecognized commnads will also be processed in this func */
        parse_cmd(argc, argv, arg_patten, &arg_value);
        process_cmd((const char *)arg_patten, &arg_value);
    }

    return 0;
}

static void print_advise(AdviseType advise_type)
{
    switch (advise_type) {
        case HELP:
            printf("HINT: try '-h' or '-?' for more infromation.\n");
            break;
        case INPUT_PASSWD:
            printf("WARNING：it is safer to enter password interactively than use '-p'\n");
        default:
            break;
    }
}

static void print_kt_version(void)
{
    printf("%s\n", KT_VERSION);
}

static void print_help_info(void)
{
    printf("Description: gs_ktool is a CMK(Client Master Key) & RK(Root Key) management tool.\n");
    printf("\n");
    printf("Usage:\n");
    printf("    gs_ktool [OPTION] [[CMK_ID] \"all\" [FILE] [FILE_PASSWD] ...]\n");
    printf("\n");
    printf("General options:\n");
    printf("    %-7s  %-40s\n", "-v", "Select gs_ktool version.");
    printf("    %-7s  %-40s\n", "-h/-?", "Select help info.");
    printf("\n");
    printf("CMK management options:\n");
    printf("    %-7s  %-40s\n", "-g", "Generate a CMK, can use '-l' to set key length.");
    printf("    %-7s  %-40s\n", "-l", "Set the key length while generating CMK.");
    printf("    %-7s  %-40s\n", "-d", "Delete specified CMK or delete all.");
    printf("    %-7s  %-40s\n", "-s", "Select specified CMK info or select all.");
    printf("    %-7s  %-40s\n", "-e",
        "Export all CMKs to ksf, can use '-f' to set export file, can use '-p' to set password.");
    printf("    %-7s  %-40s\n", "-i", 
        "Import all CMKs from ksf, should use '-f' to specify import file, can use '-p' to specify password.");
    printf("    %-7s  %-40s\n", "-f", "Set or specify key store file while exporting or importing all keys.");
    printf("    %-7s  %-40s\n", "-p",
        "Set or specify the password of key store file while exporting or importing all keys.");
    printf("\n");
    printf("RK management options:\n");
    printf("    %-7s  %-40s\n", "-R -s", "Select RK info.");
    printf("    %-7s  %-40s\n", "-R -u", "Update RK.");
}

static void parse_cmd(int argc, char *argv[], char *arg_patten, ArgValue *arg_value)
{
    errno_t rc = 0;
    bool is_passwd_too_long = false;

    /* extern global variables in "getopt.h" */
    optind = 1;
    /* optarg */

    const char *optstring = "vhgl:d:s:ief:p:";
    static struct option long_options[] = {0}; /* remain */
    int cur_arg = 0;
    int cur_pos = 0;

    arg_value->has_set_all = false;

    while (optind < argc) {
        cur_pos++;

        cur_arg = getopt_long(argc, argv, optstring, long_options, NULL);
        if (cur_arg == -1) {
            insert_format_log(L_PARSE_USER_INPUT, KT_HINT, "unknown option '%s'", argv[cur_pos]);
            print_advise(HELP);
            exit(1);
        }

        switch (cur_arg) {
            case 'v':
                printf("VERSION\n");
                print_kt_version();
                exit(0);
            case 'h':
                printf("HELP\n");
                print_help_info();
                exit(0);
            case 'g':
                arg_patten[ARG_G]++;
                break;
            case 'l':
                check_user_input(I_KEY_LEN, optarg);
                arg_value->cmk_len = atoi(optarg);
                arg_patten[ARG_L]++;
                break;
            case 'd':
                check_user_input(I_KEY_ID, optarg);
                if (strcmp(optarg, "all") == 0) {
                    arg_value->has_set_all = true;
                } else {
                    arg_value->cmk_id = atoi(optarg);
                }
                arg_patten[ARG_D]++;
                break;
            case 's':
                check_user_input(I_KEY_ID, optarg);
                if (strcmp(optarg, "all") == 0) {
                    arg_value->has_set_all = true;
                } else {
                    arg_value->cmk_id = atoi(optarg);
                }
                arg_patten[ARG_S]++;
                break;
            case 'i':
                arg_patten[ARG_I]++;
                break;
            case 'e':
                arg_patten[ARG_E]++;
                break;
            case 'f':
                check_user_input(I_KSF, optarg);
                rc = strcpy_s(arg_value->ksf, sizeof(arg_value->ksf), optarg);
                securec_check_c(rc, "", "");
                arg_patten[ARG_F]++;
                break;
            case 'p':
                print_advise(INPUT_PASSWD);
                if (strlen(optarg) >= MAX_KSF_PASS_BUF_LEN) {
                    is_passwd_too_long = true;
                    insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the password is too long");
                } else {
                    rc = strcpy_s(arg_value->ksf_passwd, sizeof(arg_value->ksf_passwd), optarg);
                    securec_check_c(rc, "", "");
                    arg_patten[ARG_P]++;
                }

                rc = memset_s(optarg, strlen(optarg), 0, strlen(optarg));
                securec_check_c(rc, "", "");
                if (is_passwd_too_long) {
                    exit(1);
                }
                break;
            case '?':
                insert_format_log(L_PARSE_USER_INPUT, KT_HINT, "unknown option '%s'", argv[cur_pos]);
                print_advise(HELP);
                exit(1);
            default:
                print_advise(HELP);
                exit(1);
        }
    }
}

static void parse_rk_manage_cmd(int argc, char *argv[])
{
    /* extern global variables in "getopt.h" */
    optind = 1;
    /* optarg */
    int option_index;

    const char *optstring = "Rsu";
    static struct option long_options[] = {0};
    int cur_arg = 0;
    int cur_pos = 0;
    unsigned char is_exit = NOT_EXIT;
    RkInfo rk_info = { 0 };

    if (argc != 3) {
        insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "unrecognized root key management option combination");
        print_advise(HELP);
        exit(1);
    }

    if (!init_keytool()) {
        exit(1);
    }

    while (is_exit == NOT_EXIT) {
        is_exit = EXIT;
        cur_arg = getopt_long(argc, argv, optstring, long_options, &option_index);
        cur_pos++;

        if (cur_arg == -1) {
            insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "unknown root key management option '%s'", argv[cur_pos]);
            print_advise(HELP);
            break;
        }

        switch (cur_arg) {
            case 'R':
                is_exit = NOT_EXIT;
                break;
            case 's':
                if (select_rk_info(&rk_info)) {
                    insert_format_log(L_SELECT_RK, KT_SUCCEED, "");
                    print_rk_info(&rk_info);
                } else {
                    insert_format_log(L_SELECT_RK, KT_ERROR, "");
                }
                break;
            case 'u':
                if (cycle_rk()) {
                    printf("UPDATE ROOTKEY\n");
                    insert_format_log(L_UPDATE_RK, KT_SUCCEED, "");
                } else {
                    insert_format_log(L_UPDATE_RK, KT_ERROR, "");
                }
                break;
            default:
                print_advise(HELP);
                break;
        }
    }

    WsecFinalizeEx();
}

static bool get_passwd(char *passwd, const size_t pwbuf_size)
{
    char *tmp_buf = NULL;
    error_t rc = 0;
    
    if (strlen(passwd) > 0) {
        return true;
    }

    tmp_buf = getpass("please input the password of key store file :");
    if (tmp_buf == NULL) {
        insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "cannot get password");
        return false;
    }

    rc = strcpy_s(passwd, pwbuf_size, tmp_buf);
    securec_check_c(rc, "", "");

    rc = memset_s(tmp_buf, strlen(tmp_buf), 0, strlen(tmp_buf));
    securec_check_c(rc, "", "");
    kt_free(tmp_buf);

    return true;
}

static PasswdErrType check_passwd(char *ksf_passwd)
{
    char chara_type_cnt = 0;
    bool has_chara_type[4] = {false, false, false, false}; /* [num, upper letter, lower letter, symbol] */
    const char legal_symbol[] = " !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
    bool is_legal_symbol = false;
    error_t rc = 0;

    if (strlen(ksf_passwd) < MIN_KSF_PASS_LEN || strlen(ksf_passwd) >= MAX_KSF_PASS_BUF_LEN) {
        rc = memset_s(ksf_passwd, strlen(ksf_passwd), 0, strlen(ksf_passwd));
        securec_check_c(rc, "", "");
        return INVALID_LEN;
    }

    for (size_t i = 0; i < strlen(ksf_passwd); i++) {
        if (ksf_passwd[i] >= '0' && ksf_passwd[i] <= '9') {
            has_chara_type[NUM] = true;
        } else if (ksf_passwd[i] >= 'A' && ksf_passwd[i] <= 'Z') {
            has_chara_type[UPPER] = true;
        } else if (ksf_passwd[i] >= 'a' && ksf_passwd[i] <= 'z') {
            has_chara_type[LOWER] = true;
        } else {
            for (size_t j = 0; j < sizeof(legal_symbol) / sizeof(legal_symbol[0]); j++) {
                if (ksf_passwd[i] == legal_symbol[j]) {
                    is_legal_symbol = true;
                    has_chara_type[SYMBOL] = true;
                    break;
                }
            }

            if (is_legal_symbol == false) {
                rc = memset_s(ksf_passwd, strlen(ksf_passwd), 0, strlen(ksf_passwd));
                securec_check_c(rc, "", "");
                return INVALID_CHAR;
            }

            is_legal_symbol = false;
        }
    }

    for (size_t i = 0; i < sizeof(has_chara_type) / sizeof(has_chara_type[0]); i++) {
        chara_type_cnt += has_chara_type[i];
    }

    if (chara_type_cnt < MIN_KSF_PASS_CHAR_TYPE) {
        rc = memset_s(ksf_passwd, strlen(ksf_passwd), 0, strlen(ksf_passwd));
        securec_check_c(rc, "", "");
        return FEW_TYPES;
    }

    return LEGAL;
}

static void report_passwd_err(PasswdErrType passwd_err)
{   
    switch (passwd_err) {
        case INVALID_LEN:
            insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the password length should be in range [%d, %d)",
                MIN_KSF_PASS_LEN, MAX_KSF_PASS_BUF_LEN);
            break;
        case INVALID_CHAR:
            insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the password contains invalid character");
            break;
        case FEW_TYPES:
            insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the password should contain 3 types of characters");
            break;
        default:
            break;
    }
}

static void check_user_input(const InputType input_type, const char *user_input)
{
    char real_path_file[MAX_REAL_PATH_LEN] = { 0 };
    unsigned int input_num = 0;

    switch (input_type) {
        case I_KEY_ID:
            if (strcmp(optarg, "all") == 0 || strcmp(optarg, "all\n") == 0) {
                break;
            } else {
                if (!atoi_strictly(user_input, &input_num)) {
                    insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, 
                        "this input '%s' is expected to be an positive integer", user_input);
                    exit(1);
                }
                if (input_num < 1 || input_num > MAX_CMK_QUANTITY) {
                    insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the key id '%s' should be in range [1, %d]",
                        user_input, MAX_CMK_QUANTITY);
                    exit(1);
                }
            }
            break;
        case I_KEY_LEN:
            if (!atoi_strictly(user_input, &input_num)) {
                insert_format_log(L_CHECK_USER_INPUT, KT_ERROR,
                    "this input '%s' is expected to be an positive integer", user_input);
                exit(1);
            }
            if (input_num < MIN_CMK_LEN || input_num > MAX_CMK_LEN) {
                insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the key len '%s' should be in range [%d, %d]", 
                    user_input, MIN_CMK_LEN, MAX_CMK_LEN);
                exit(1);
            } 
            break;
        case I_KSF:
            if (strlen(user_input) >= MAX_REAL_PATH_LEN) {
                insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the length of key store file name is too long");
                exit(1);
            }

            realpath(user_input, real_path_file);
            if (!check_kt_env_value(real_path_file)) {
                insert_format_log(L_CHECK_USER_INPUT, KT_ERROR, "the path '%s' is invalid", real_path_file);
                exit(1);
            }
            break;
        default:
            break;
    }
}

static bool is_arg_patten_equal(const char base_list[CMK_ARG_CNT], const char new_list[CMK_ARG_CNT])
{
    for (int i = 0; i < CMK_ARG_CNT; i++) {
        if (base_list[i] != new_list[i]) {
            if (new_list[i] > 1) {
                insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "too many options : '%s'", g_cmk_manage_arg[i]);
                exit(1);
            }
            return false;
        }
    }

    return true;
}

static bool reg_cmk_manage_func(CmkManageFuncList *cmk_manage_func_list, CmkManageFunc *new_func_node)
{
    CmkManageFunc *last_node = NULL;

    if (cmk_manage_func_list == NULL || new_func_node == NULL) {
        return false;
    }

    if (cmk_manage_func_list->first_node == NULL) {
        cmk_manage_func_list->first_node = new_func_node;
        cmk_manage_func_list->node_cnt++;
        return true;
    }

    last_node = cmk_manage_func_list->first_node;
    while (last_node->next != NULL) {
        last_node = last_node->next;
    }

    last_node->next = new_func_node;
    cmk_manage_func_list->node_cnt++;
    return true;
}

static CmkManageFunc *create_cmk_manage_func(const char arg_patten[CMK_ARG_CNT], ManageCmkHookFunc manage_cmk_func)
{
    errno_t rc = 0;
    CmkManageFunc *cmk_manage_func = NULL;

    cmk_manage_func = (CmkManageFunc *)malloc(sizeof(CmkManageFunc));
    if (cmk_manage_func == NULL) {
        insert_format_log(L_MALLOC_MEMORY, KT_ERROR, "");
        return NULL;
    }

    rc = memcpy_s(cmk_manage_func->arg_patten, CMK_ARG_CNT, arg_patten, CMK_ARG_CNT);
    securec_check_c(rc, "", "");

    cmk_manage_func->manage_cmk_func = manage_cmk_func;
    cmk_manage_func->next = NULL;

    return cmk_manage_func;
}

static void destroy_cmk_manage_func_list(CmkManageFuncList *cmk_manage_func_list)
{
    CmkManageFunc *cur_node = NULL;
    CmkManageFunc *to_free = NULL;

    cur_node = cmk_manage_func_list->first_node;
    while (cur_node != NULL) {
        to_free = cur_node;
        cur_node = cur_node->next;
        kt_free(to_free);
    }
}

static void reg_all_cmk_manage_func(CmkManageFuncList *cmk_manage_func_list)
{
    const char support_patten[][CMK_ARG_CNT] = {
        /* g l d  s  i  e  f  p */
        {1, 0, 0, 0, 0, 0, 0, 0},
        {1, 1, 0, 0, 0, 0, 0, 0},
        {0, 0, 1, 0, 0, 0, 0, 0},
        {0, 0, 0, 1, 0, 0, 0, 0},
        {0, 0, 0, 0, 1, 0, 1, 1},
        {0, 0, 0, 0, 1, 0, 1, 0},
        {0, 0, 0, 0, 0, 1, 0, 0},
        {0, 0, 0, 0, 0, 1, 0, 1},
        {0, 0, 0, 0, 0, 1, 1, 1},
        {0, 0, 0, 0, 0, 1, 1, 0},
    };
    const ManageCmkHookFunc manage_cmk_func[] = {
        process_gen_cmk_cmd,
        process_gen_cmk_cmd,
        process_del_cmk_cmd,
        process_select_cmk_cmd,
        process_import_cmk_cmd,
        process_import_cmk_cmd,
        process_export_cmk_cmd,
        process_export_cmk_cmd,
        process_export_cmk_cmd,
        process_export_cmk_cmd,
    };
    CmkManageFunc *new_func_node = NULL;

    for (size_t i = 0; i < sizeof(support_patten) / sizeof(support_patten[0]); i++) {
        new_func_node = create_cmk_manage_func(support_patten[i], manage_cmk_func[i]);
        if (!reg_cmk_manage_func(cmk_manage_func_list, new_func_node)) {
            kt_free(new_func_node);
            destroy_cmk_manage_func_list(cmk_manage_func_list);
            exit(1);
        }
    }
}

static void process_cmd(const char arg_patten[CMK_ARG_CNT], ArgValue *arg_value)
{
    CmkManageFuncList cmk_manage_func_list = {0, NULL};
    CmkManageFunc *cur_func = NULL;
    bool is_arg_patten_legal = false;

    /* step 1 : register key manage function */
    reg_all_cmk_manage_func(&cmk_manage_func_list);

    cur_func = cmk_manage_func_list.first_node;
    while (cur_func != NULL) {
        if (is_arg_patten_equal(cur_func->arg_patten, arg_patten)) {
            is_arg_patten_legal = true;
            
            /* step 2 : init gs_ktool */
            if (!init_keytool()) {
                break;
            }

            /* step 3 : call key manage function to process arg_value */
            cur_func->manage_cmk_func(arg_value);
            break;
        }
        cur_func = cur_func->next;
    }

    if (is_arg_patten_legal == false) {
        insert_format_log(L_PARSE_USER_INPUT, KT_ERROR, "unrecognized option combination");
        print_advise(HELP);
    }

    /* step 4 : clean call back functions and kmc */
    destroy_cmk_manage_func_list(&cmk_manage_func_list);
    WsecFinalizeEx();
}

static void process_gen_cmk_cmd(ArgValue *arg_value)
{
    unsigned int cmk_id = 0;

    if (arg_value->cmk_len == 0) {
        if (generate_cmk(false, UNUSED, &cmk_id)) {
            printf("GENERATE\n%u\n", cmk_id);
            insert_format_log(L_GEN_CMK, KT_SUCCEED, "new cmk : %u", cmk_id);
        } else {
            insert_format_log(L_GEN_CMK, KT_ERROR, "");
        }
    } else {
        if (generate_cmk(true, arg_value->cmk_len, &cmk_id)) {
            printf("GENERATE\n%u\n", cmk_id);
            insert_format_log(L_GEN_CMK, KT_SUCCEED, "new cmk : %d, cmk len : %d", cmk_id, arg_value->cmk_len);
        } else {
            insert_format_log(L_GEN_CMK, KT_ERROR, "");
        }
    }
}

static void process_del_cmk_cmd(ArgValue *arg_value)
{
    unsigned int del_cmk_id_list[MAX_CMK_QUANTITY] = {0};
    unsigned int del_cmk_cnt = 0;

    if (arg_value->has_set_all == true) {
        if (delete_cmk(true, UNUSED, del_cmk_id_list, &del_cmk_cnt)) {
            printf("DELETE ALL\n");
            for (unsigned int i = 0; i < del_cmk_cnt; i++) {
                printf(" %u", del_cmk_id_list[i]);
            }
            printf("\n");
            insert_format_log(L_DEL_CMK, KT_SUCCEED, "deleted all")
        } else {
            insert_format_log(L_DEL_CMK, KT_ERROR, "");
        }
    } else {
        if (delete_cmk(false, arg_value->cmk_id, { 0 }, &del_cmk_cnt)) {
            printf("DELETE\n%u\n", arg_value->cmk_id);
            insert_format_log(L_DEL_CMK, KT_SUCCEED, "delete cmk : %d", arg_value->cmk_id);
        } else {
            insert_format_log(L_DEL_CMK, KT_ERROR, "which id is '%s'", optarg);
        }
    }
}

static void process_select_cmk_cmd(ArgValue *arg_value)
{
    CmkInfo *cmk_info_list = NULL;

    if (arg_value->has_set_all == true) {
        cmk_info_list = select_cmk_info(true, UNUSED);
        if (cmk_info_list == NULL) {
            insert_format_log(L_SELECT_CMK, KT_ERROR, "");
            return;
        } else {
            printf("SELECT ALL\n");
            trav_cmk_info_list(cmk_info_list);
            insert_format_log(L_SELECT_CMK, KT_SUCCEED, "select all");
        }
    } else {
        cmk_info_list = select_cmk_info(false, arg_value->cmk_id);
        if (cmk_info_list == NULL) {
            insert_format_log(L_SELECT_CMK, KT_ERROR, "which id is '%s'", optarg);
            return;
        } else {
            printf("SELECT\n");
            trav_cmk_info_list(cmk_info_list);
            insert_format_log(L_SELECT_CMK, KT_SUCCEED, "select : %d", arg_value->cmk_id);
        }
    }

    destroy_cmk_info_list(cmk_info_list);
}

static void process_import_cmk_cmd(ArgValue *arg_value)
{
    unsigned int ksf_pass_len = 0;
    CmkInfo *cmk_info_list = NULL;
    PasswdErrType pw_err = LEGAL;
    
    if (!get_passwd(arg_value->ksf_passwd, sizeof(arg_value->ksf_passwd))) {
        return;
    }

    pw_err = check_passwd(arg_value->ksf_passwd);
    if (pw_err != LEGAL) {
        insert_format_log(L_IMPORT_CMK, KT_ERROR, 
            "please make sure the key store file '%s' is legal and the password is correct", arg_value->ksf);
        return;
    }

    ksf_pass_len = strlen(arg_value->ksf_passwd);
    if (import_cmks_from_ksf(arg_value->ksf, (unsigned char *)arg_value->ksf_passwd, ksf_pass_len)) {
        printf("SAFE IMPORT\n%s\n", arg_value->ksf);
        cmk_info_list = select_cmk_info(true, UNUSED);
        if (cmk_info_list == NULL) {
            insert_format_log(L_SELECT_CMK, KT_ERROR, "");
            return;
        } else {
            trav_cmk_info_list(cmk_info_list);
            insert_format_log(L_SELECT_CMK, KT_SUCCEED, "select all");
        }
        insert_format_log(L_IMPORT_CMK, KT_SUCCEED, "import from : '%s'", arg_value->ksf);
    } else {
        insert_format_log(L_IMPORT_CMK, KT_ERROR, 
            "please make sure the key store file '%s' is legal and the password is correct", arg_value->ksf);
    }

    destroy_cmk_info_list(cmk_info_list);
}

static void process_export_cmk_cmd(ArgValue *arg_value)
{
    unsigned int ksf_pass_len = 0;
    bool ret = false;
    PasswdErrType pw_err = LEGAL;
    errno_t rc = 0;
    
    if (!get_passwd(arg_value->ksf_passwd, sizeof(arg_value->ksf_passwd))) {
        return;
    }

    pw_err = check_passwd(arg_value->ksf_passwd);
    if (pw_err != LEGAL) {
        report_passwd_err(pw_err);
        return;
    }

    ksf_pass_len = strlen(arg_value->ksf_passwd);

    if (strlen(arg_value->ksf) > 0) {
        ret = export_cmks_to_ksf(true, arg_value->ksf, (unsigned char *)arg_value->ksf_passwd, ksf_pass_len);
    } else {
        ret = export_cmks_to_ksf(false, NULL, (unsigned char *)arg_value->ksf_passwd, ksf_pass_len);
        rc = strcpy_s(arg_value->ksf, sizeof(arg_value->ksf), "path configured in gs_ktool_conf.ini");
        securec_check_c(rc, "", "");
    }

    if (ret) {
        printf("SAFE EXPORT\n%s\n", arg_value->ksf);
        insert_format_log(L_EXPORT_CMK, KT_SUCCEED, "exported to : '%s'", arg_value->ksf);
    } else {
        insert_format_log(L_EXPORT_CMK, KT_ERROR, "please check the path : '%s'", arg_value->ksf);
    }
}