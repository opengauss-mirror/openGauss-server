/*
 * Copyright (c) 2020-2025 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * gs_loader.cpp
 * 	        gs_loader will call gs_loader.sh.
 * The purpose of the file is to hide the username and password
 *
 * IDENTIFICATION
 * src/bin/gs_loader/gs_loader.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <string>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <regex>
#include <unistd.h>
namespace gs_loader {
static const char param_key0[] = "passwd=";
static const char param_key1[] = "-W";
// key max len 1024 , 1 for \0
static const int MAX_KEY_LEN = 1024;

// shell script file
static const std::string GS_LOADER_FILE = "gs_loader.sh";
bool FindKey(const char *arr1, int n1, const char *key, int n)
{
    if (n1 < n) {
        return false;
    }
    for (int i = 0; i < n; i++) {
        if (arr1[i] != key[i]) {
            return false;
        }
    }
    return true;
}
void GetKey1(const char *arr1, int n1, char *key)
{
    for (int i = 0; i < n1; i++) {
        key[i] = arr1[i];
    }
    key[n1] = '\0';
}

void GetKey0(const char *arr1, int n1, char *key)
{
    int i, j;
    for (i = 0; i < n1; i++) {
        if (arr1[i] == '=') {
            break;
        }
    }
    for (j = i + 1; j < n1; j++) {
        key[j - i - 1] = arr1[j];
    }
    key[j - i - 1] = '\0';
}

void SetX(char *arr1, int n1)
{
    for (int i = 0; i < n1; i++) {
        arr1[i] = 'x';
    }
}

bool InputPasswd(char *keyword, int max_len)
{
    char *password = getpass("Input a password:");
    if (password == NULL) {
        std::cout << "ERROR: read password error";
        return false;
    }
    if (strlen(password) > (unsigned)max_len) {
        std::cout << "ERROR: read password error";
        SetX(password, strlen(password));
        return false;
    }
    for (unsigned i = 0; i < strlen(password); i++) {
        keyword[i] = password[i];
        password[i] = 'x';
    }
    return true;
}

void check_danger_character(const char *inputEnvValue)
{
    if (inputEnvValue == NULL)
        return;

    const char* dangerCharacterList[] = {";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", NULL};
    int i = 0;

    for (i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr(inputEnvValue, dangerCharacterList[i]) != NULL) {
            fprintf(stderr, "ERROR: Failed to check input value: invalid token \"%s\".\n", dangerCharacterList[i]);
            exit(1);
        }
    }
}
}

using namespace gs_loader;

int main(int argc, char **argv)
{
    char keyword[MAX_KEY_LEN + 1] = {'\0'};
    bool have_passwd = false;

    // path size include \0
    const int PATH_SIZE = 1024;

    // get the exe path for get shell script
    char abs_path[PATH_SIZE] = {'\0'};
    int cnt = readlink("/proc/self/exe", abs_path, PATH_SIZE);
    if (cnt == -1 || cnt >= PATH_SIZE) {
        std::cout << "ERROR: can not find gs_loader path" << std::endl;
        return 0;
    }
    abs_path[cnt] = '\0';

    std::string exe_path(abs_path);
    int path_pos = exe_path.rfind("/");
    exe_path = exe_path.substr(0, path_pos + 1);
    std::string params = exe_path + GS_LOADER_FILE;


    for (int i = 1; i < argc; i++) {
        std::string param(argv[i]);
        // get sensitive info and hide them

        if (FindKey(argv[i], strlen(argv[i]), param_key0, strlen(param_key0))) {
            if (strlen(argv[i]) > strlen(param_key0) + MAX_KEY_LEN) {
                std::cout << "ERROR: passwd len too long, limit:" << MAX_KEY_LEN << std::endl;
                return 0;
            }
            GetKey0(argv[i], strlen(argv[i]), keyword);
            SetX(argv[i], strlen(argv[i]));
            have_passwd = true;
        } else if (FindKey(argv[i], strlen(argv[i]), param_key1, strlen(param_key1))) {
            i++;
            if (i < argc) {
                if (strlen(argv[i]) > MAX_KEY_LEN) {
                    std::cout << "ERROR: passwd len too long, limit:" << MAX_KEY_LEN << std::endl;
                    return 0;
                }
                GetKey1(argv[i], strlen(argv[i]), keyword);
                SetX(argv[i], strlen(argv[i]));
                have_passwd = true;
            } else {
                std::cout << "ERROR:requires an argument -- \'W\'" << std::endl;
                return 0;
            }
        } else {
            params = params + " " + param;
        }
    }
    if (!have_passwd) {
        if (!InputPasswd(keyword, MAX_KEY_LEN)) {
            std::cout << "ERROR: read password error";
            return 0;
        }
    }
    const char *p = const_cast<char *>(params.c_str());
    check_danger_character(p);

    FILE *fp = popen(params.c_str(), "w");
    if (fp == NULL) {
        for (unsigned i = 0; i < strlen(keyword); i++) {
            keyword[i] = 'x';
        }
        std::cout << "ERROR: run gs_loader error" << std::endl;
        return 0;
    }
    fputs(keyword, fp);
    fputc('\n', fp);
    for (unsigned i = 0; i < strlen(keyword); i++) {
        keyword[i] = 'x';
    }
    pclose(fp);
    return 0;
}
