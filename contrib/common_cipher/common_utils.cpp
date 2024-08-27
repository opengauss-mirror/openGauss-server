#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "common_utils.h"
#include "common_err.h"
#include "common_algo.h"

/*去掉字符串头和尾的空格*/
static char* remove_space(char *input)
{
    int header = 0;
    int end = 0;
    int inputlen = 0;
    const char filter = ' ';
    char *tmp = NULL;

    if (input == NULL || input[0] == '\0') {
        return NULL;
    }

    inputlen = strlen(input);

    /*去头部空格*/
    while(input[header++] == filter && header < inputlen) {
        /*do nothing*/
    }

    end = inputlen - 1;

    /*去尾部空格*/
    while(input[end--] == filter && end >= (header - 1)) {
        /*do nothing*/
    }

    tmp = (char*)malloc(inputlen + 1);
    memset(tmp,0x0,inputlen + 1);

    /*初始位置为header - 1,长度为（end + 1）- (header -1) + 1*/
    memcpy(tmp, input + header - 1, end - header + 3);

    return tmp;
}

static int set_module_params(char* k, char* v, ModuleParams *moduleparams)
{
    if (IS_MODULE_TYPE(k)) {
        if (moduleparams->moduletype != MODULE_INVALID_TYPE) {
            return CRYPTO_MOD_TYPE_REPEATED_ERR;
        }

        moduleparams->moduletype = GET_MODULE_TYPE(v);

        if (moduleparams->moduletype == MODULE_INVALID_TYPE) {
            return CRYPTO_MOD_TYPE_INVALID_ERR;
        }
    } else if (IS_MODULE_LIB_PATH(k)) {

        if (moduleparams->libpath[0] != '\0') {
            return CRYPTO_MOD_LIBPATH_REPEATED_ERR;
        }

        if (v[0] == '\0' || strlen(v) >= MODULE_MAX_PATH_LEN) {
            return CRYPTO_MOD_LIBPATH_INVALID_ERR;
        } else {
            memcpy(moduleparams->libpath, v, strlen(v));
        }
    } else if(IS_MODULE_CONFIG_FILE_PATH(k)) {
        
        if (moduleparams->cfgfilepath[0] != '\0') {
            return CRYPTO_MOD_CFG_PATH_REPEATED_ERR;
        }

        if (v[0] == '\0' || strlen(v) >= MODULE_MAX_PATH_LEN) {
            return CRYPTO_MOD_CFG_PATH_INVALID_ERR;
        } else {
            memcpy(moduleparams->cfgfilepath, v, strlen(v));
        }
    } else {
        return CRYPTO_MOD_UNKNOWN_PARAM_ERR;
    }

    return CRYPT_MOD_OK;

}

static int check_module_params(ModuleParams *moduleparams)
{
    if (moduleparams->libpath[0] == '\0') {
        return CRYPTO_MOD_LIBPATH_INVALID_ERR;
    }

    switch (moduleparams->moduletype) {
        case MODULE_GDAC_CARD_TYPE:
            /*光电安辰密码卡不需要配置文件*/
            if (moduleparams->cfgfilepath[0] != '\0') {
                return CRYPTO_MOD_PARAM_TOO_MANY_ERR;
            }
            break;
        case MODULE_JNTA_KMS_TYPE:
            /*江南天安的配置文件，需要设置为环境变量使用"TASS_GHVSM_CFG_PATH",在后面加载时自己临时设置使用*/
            if (moduleparams->cfgfilepath[0] == '\0') {
                return CRYPTO_MOD_CFG_PATH_INVALID_ERR;
            }
            break;
        case MODULE_SWXA_KMS_TYPE:
            /*三未信安kms使用带配置文件路径名称的接口*/
            if (moduleparams->cfgfilepath[0] == '\0') {
                return CRYPTO_MOD_CFG_PATH_INVALID_ERR;
            }
            break;
        default:
            return CRYPTO_MOD_TYPE_INVALID_ERR;
    }

    return CRYPT_MOD_OK;
}

int parse_module_params(char *paramsstring, ModuleParams *moduleparams)
{
    char *p = NULL;
    char *saveptr1 = NULL;
    const char *split1 = ",";
    const char *split2 = "=";
    int ret = 0;

    p = strtok_r(paramsstring, split1, &saveptr1);

    while (p != NULL) {
        char *q = NULL;
        char *saveptr2 = NULL;
        char *tmp_p = NULL;
        char *tmp_ptr2 = NULL;
        q = strtok_r(p, split2, &saveptr2);

        tmp_p = remove_space(p);
        tmp_ptr2 = remove_space(saveptr2);

        ret = set_module_params(tmp_p, tmp_ptr2, moduleparams);
        if (ret != CRYPT_MOD_OK) {
            free(tmp_p);
            free(tmp_ptr2);
            return ret;
        }

        free(tmp_p);
        free(tmp_ptr2);

        q = NULL;
        p = strtok_r(NULL, split1, &saveptr1);
    }

    return check_module_params(moduleparams);

}

