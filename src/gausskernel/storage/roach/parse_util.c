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
 * -------------------------------------------------------------------------
 *
 * parse_util.cpp
 *    roach parse method definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/roach/parse_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdio.h>
#include <getopt.h>
#include "parse_util.h"

/**
* @brief    get configuration from cfg file.
* @param    path
* @param    options
* @param    bParsePostgres
* @return

* @exception    None

* @date     2015/3/9
*/
ERROR_CODE roachReadopt(const char* path, roach_option opts[], size_t optsNum, bool bParsePostgres)
{
    FILE* fp = NULL;
    char buf[MAX_BUF_SZ] = {0};
    char key[MAX_BUF_SZ] = {0};
    char value[MAX_BUF_SZ] = {0};

    /* If no options passed
     */
    if (!opts) {
        return EC_SUCCESS;
    }

    /* Open the cfg file
     */
    if ((fp = FOPEN(path, "rb")) == NULL) {
        LOGERROR("could not open file \"%s\": %s", path, gs_strerror(errno));
        return EC_FILE_OPEN_FAILED;
    }

    /* Read the configuration from file
     */
    while (fgets(buf, lengthof(buf), fp)) {
        size_t i;

        for (i = strlen(buf); i > 0 && isspace(buf[i - 1]); i--) {
            buf[i - 1] = '\0';
        }

        /* Parse the configuration content
         */
        if (parse_pair(buf, key, value, MAX_BUF_SZ, MAX_BUF_SZ, MAX_BUF_SZ)) {
            for (i = 0; (i < optsNum) && opts[i].type; i++) {
                roach_option* opt = &opts[i];

                /* Check if config matches the config passed
                 */
                if (key_equals(key, opt->lname)) {
                    if (opt->allowed == SOURCE_DEFAULT || opt->allowed > SOURCE_FILE) {
                        LOGERROR("option %s cannot specified in file %s", opt->lname, path);
                        fclose(fp);
                        return EC_INVALID_ARGUMENT;
                    } else if (opt->source <= SOURCE_FILE) {
                        /* Get the configuration value
                         */
                        if (getArgvValueForOption(opt, value, SOURCE_FILE) != EC_SUCCESS) {
                            fclose(fp);
                            return EC_INVALID_ARGUMENT;
                        }
                    }

                    break;
                }
            }
        }
    }

    fclose(fp);
    return EC_SUCCESS;
}

/**
 * parse_next_token_nonspl
 * @brief    get the token from the string (other than spl control chars)
 * @param    src
 * @param    dst
 * @param    i
 * @param    j
 * @return
 * */
void parse_next_token_nonspl(const char* src, char* dst, size_t i, size_t j)
{
    switch (src[i]) {
        case '0':
        case '1':
        case TWO:
        case THREE:
        case FOUR:
        case FIVE:
        case SIX:
        case SEVEN: {
            size_t k;
            uint64_t octVal = 0;

            for (k = 0; src[i + k] >= '0' && src[i + k] <= SEVEN && k < SHIFT_THREE; k++) {
                octVal = (octVal << SHIFT_THREE) + (src[i + k] - '0');
            }

            i += k - 1;
            if (octVal < 128) {
                dst[j] = ((char)octVal);
            }
        } break;
        default:
            dst[j] = src[i];
            break;
    }

    UNUSED(i);
}

/**
 * parse_next_token
 * @brief    get the token from the string
 * @param    src
 * @param    dst
 * @param    i
 * @param    j
 * @return
 * */
void parse_next_token(const char* src, char* dst, size_t i, size_t j)
{
    if (src[i] == 'b') {
        dst[j] = '\b';
    } else if (src[i] == 'f') {
        dst[j] = '\f';
    } else if (src[i] == 'n') {
        dst[j] = '\n';
    } else if (src[i] == 'r') {
        dst[j] = '\r';
    } else if (src[i] == 't') {
        dst[j] = '\t';
    } else {
        parse_next_token_nonspl(src, dst, i, j);
    }

    UNUSED(i);
}

/**
* @brief    parse the key and values
* @param    buffer
* @param    key
* @param    value
* @return

* @exception    None

* @date     2015/3/9
*/
bool parse_pair(const char buffer[], char key[], char value[], size_t lenBuf, size_t lenKey, size_t lenValue)
{
    const char* start = NULL;
    const char* end = NULL;
    int nRet = 0;
    size_t len;
    key[0] = value[0] = '\0';

    /*
     * parse key
     */
    start = buffer;
    len = strcspn(start, "=# \n\r\t\v");
    if (len > lenKey) {
        LOGERROR("Key length %lu is more than Maximum length allowed", len);
        return false;
    }
    if (*(start = skip_space(start)) == '\0') {
        return false;
    }

    end = start + strcspn(start, "=# \n\r\t\v");
    /* skip blank buffer */
    if (end - start <= 0) {
        if (*start == '=') {
            LOGERROR("syntax error in \"%s\"", buffer);
        }

        return false;
    }

    /* key found */
    nRet = strncpy_s(key, lenKey, start, (size_t)(end - start));
    securec_check_c(nRet, "\0", "\0");
    key[end - start] = '\0';

    /*
    fgets() fucntion call always takes care of data read and its length, so
    end - start can always be less than MAX_BUF_SZ so there won't be any overlapping could occur.
    */

    /* find key and value split char */
    if (*(start = skip_space(end)) == '\0') {
        return false;
    }

    if (*start != '=') {
        LOGERROR("syntax error in \"%s\"", buffer);
        return false;
    }

    start++;

    /*
     * parse value
     */
    if (*(start = skip_space(start)) == '\0') {
        return false;
    }

    len = strcspn(start, "# \n\r\t\v");
    if (len > lenValue) {
        LOGERROR("Value length %lu is more than Maximum length allowed", len);
        return false;
    }
    if ((end = get_next_token(start, value, lenValue, buffer)) == NULL) {
        return false;
    }

    if (*(start = skip_space(end)) == '\0') {
        return false;
    }

    if (*start != '\0' && *start != '#') {
        LOGERROR("syntax error in \"%s\"", buffer);
        return false;
    }

    return true;
}

/**
 * parseBool
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 * @param value
 * @param result
 * @return
 */
bool parseBool(const char* value, bool* result)
{
    return parseBoolWithLen(value, strlen(value), result);
}

/**
 * parseBoolWithLen
 *
 * Parse and get the boolean values specified in the conf file.
 * Supports T/F, t/f, Y/N, y/n, on/off, ON/OFF, 1/0
 * @param value - pointer to the boolean string from conf file
 * @param len - length of the boolean value in buffer
 * @param result - boolean result
 * @return - return boolean value.
 */
bool parseBoolWithLen(const char* value, size_t len, bool* result)
{
    if (parseBoolCheckTF(value, len, result)) {
        return true;
    }

    if (parseBoolCheckYN(value, len, result)) {
        return true;
    }

    if (parseBoolCheckOnOff(value, len, result)) {
        return true;
    }

    if (parseBoolCheckOneZero(value, len, result)) {
        return true;
    }

    if (result != NULL) {
        *result = false; /* suppress compiler warning */
    }

    return false;
}

/**
 * parseBoolCheckTF
 *
 * Check "True/False" code, splitted from "parseBoolWithLen"
 * @param value - pointer to the boolean string from conf file
 * @param len - length of the boolean value in buffer
 * @param result - boolean result
 * @return - return boolean value.
 */
bool parseBoolCheckTF(const char* value, size_t len, bool* result)
{
    if (strNCaseCmp(value, "true", len) == 0) {
        if (result != NULL) {
            *result = true;
        }

        return true;
    }

    if (strNCaseCmp(value, "false", len) == 0) {
        if (result != NULL) {
            *result = false;
        }

        return true;
    }

    return false;
}

/**
 * parseBoolCheckYN
 *
 * Check "Yes/No" code, splitted from "parseBoolWithLen"
 * @param value - pointer to the boolean string from conf file
 * @param len - length of the boolean value in buffer
 * @param result - boolean result
 * @return - return boolean value.
 */
bool parseBoolCheckYN(const char* value, size_t len, bool* result)
{
    if (strNCaseCmp(value, "yes", len) == 0) {
        if (result != NULL) {
            *result = true;
        }

        return true;
    }

    if (strNCaseCmp(value, "no", len) == 0) {
        if (result != NULL) {
            *result = false;
        }

        return true;
    }

    return false;
}

/**
 * parseBoolCheckOnOff
 *
 * Check "On/Off" code, splitted from "parseBoolWithLen"
 * @param value - pointer to the boolean string from conf file
 * @param len - length of the boolean value in buffer
 * @param result - boolean result
 * @return - return boolean value.
 */
bool parseBoolCheckOnOff(const char* value, size_t len, bool* result)
{
    if (strNCaseCmp(value, "on", (len > OPT_VALUE_LEN_TWO ? len : OPT_VALUE_LEN_TWO)) == 0) {
        if (result != NULL) {
            *result = true;
        }

        return true;
    } else if (strNCaseCmp(value, "off", (len > OPT_VALUE_LEN_TWO ? len : OPT_VALUE_LEN_TWO)) == 0) {
        if (result != NULL) {
            *result = false;
        }

        return true;
    }

    return false;
}

/**
 * parseBoolCheckOneZero
 *
 * Check "1/0" code, splitted from "parseBoolWithLen"
 * @param value - pointer to the boolean string from conf file
 * @param len - length of the boolean value in buffer
 * @param result - boolean result
 * @return - return boolean value.
 */
bool parseBoolCheckOneZero(const char* value, size_t len, bool* result)
{
    if (*value == '1') {
        if (len == 1) {
            if (result != NULL) {
                *result = true;
            }
            return true;
        }
    } else if (*value == '0') {
        if (len == 1) {
            if (result != NULL) {
                *result = false;
            }
            return true;
        }
    }

    return false;
}

/**
 * parseLoggingLevel
 * Parse for the logging level
 * @param value
 * @return
 */
ROACH_LOG_LEVEL parseLoggingLevel(const char* value)
{
    const char* v = value;

    while (isspace(*v)) {
        v++;
    }

    if (strNCaseCmp(LOGGING_LEVEL_FATAL, v, sizeof(LOGGING_LEVEL_FATAL)) == 0) {
        return ROACH_LOG_FATAL;
    } else if (strNCaseCmp(LOGGING_LEVEL_ERROR, v, sizeof(LOGGING_LEVEL_ERROR)) == 0) {
        return ROACH_LOG_ERROR;
    } else if (strNCaseCmp(LOGGING_LEVEL_WARNING, v, sizeof(LOGGING_LEVEL_WARNING)) == 0) {
        return ROACH_LOG_WARNING;
    } else if (strNCaseCmp(LOGGING_LEVEL_INFO, v, sizeof(LOGGING_LEVEL_INFO)) == 0) {
        return ROACH_LOG_INFO;
    } else if (strNCaseCmp(LOGGING_LEVEL_DEBUG, v, sizeof(LOGGING_LEVEL_DEBUG)) == 0) {
        return ROACH_LOG_DEBUG;
    } else if (strNCaseCmp(LOGGING_LEVEL_DEBUG2, v, sizeof(LOGGING_LEVEL_DEBUG2)) == 0) {
        return ROACH_LOG_DEBUG2;
    }

    LOGERROR("Invalid logging level specified %s\n", value);

    return ROACH_LOG_NONE;
}

/**
 * @brief    parse the backup type
 * @param    value
 *
 * @return
 */
BACKUP_MODE_TYPE parseBackupType(const char* value)
{
    const char* v = value;

    while (isspace(*v)) {
        v++;
    }
    if (strNCaseCmp(BACKUP_TYPE_FULL, v, sizeof(BACKUP_TYPE_FULL)) == 0) {
        return BACKUP_MODE_FULL;
    } else if (strNCaseCmp(BACKUP_TYPE_LOGICAL_MTABLE, v, sizeof(BACKUP_TYPE_LOGICAL_MTABLE)) == 0) {
        return BACKUP_MODE_LOGICAL_MTABLE;
    } else if (strNCaseCmp(BACKUP_TYPE_TABLE, v, sizeof(BACKUP_TYPE_TABLE)) == 0) {
        return BACKUP_MODE_TABLE;
    } else if (strNCaseCmp(BACKUP_TYPE_INCREMENTAL, v, sizeof(BACKUP_TYPE_INCREMENTAL)) == 0) {
        return BACKUP_MODE_INCREMENTAL;
    } else {
        LOGERROR("Invalid backup type option \"%s\"", value);
        return BACKUP_MODE_INVALID;
    }
}

/**
 * parseMediaType
 * Parse for the media type
 * @param value
 * @return
 */
ROACH_MEDIA_TYPE parseMediaType(const char* media)
{
    const char* v = media;

    while (isspace(*v)) {
        v++;
    }

    if (strNCaseCmp(MEDIA_DISK, v, sizeof(MEDIA_DISK)) == 0) {
        return ROACH_MEDIA_TYPE_DISK;
    } else if (strNCaseCmp(MEDIA_NBU, v, sizeof(MEDIA_NBU)) == 0) {
        return ROACH_MEDIA_TYPE_NBU;
    } else if (strNCaseCmp(MEDIA_OBS, v, sizeof(MEDIA_OBS)) == 0) {
        return ROACH_MEDIA_TYPE_OBS;
    } else if (strNCaseCmp(MEDIA_EISOO, v, sizeof(MEDIA_EISOO)) == 0) {
        return ROACH_MEDIA_TYPE_EISOO;
    } else if (strNCaseCmp(MEDIA_NAS, v, sizeof(MEDIA_NAS)) == 0) {
        return ROACH_MEDIA_TYPE_NAS;
    } else {
        return ROACH_MEDIA_INVALID;
    }
}

/**
 * parseValidationType
 * Parse for the media type
 * @param value
 * @return
 */
ROACH_VALIDATION_TYPE parseValidationType(const char* media)
{
    const char* v = media;

    while (isspace(*v)) {
        v++;
    }

    if (strNCaseCmp(VALIDATION_TYPE_FULL, v, sizeof(VALIDATION_TYPE_FULL)) == 0) {
        clioptions.enableCrc = true;
        return ROACH_VALIDATION_TYPE_FULL;
    } else if (strNCaseCmp(VALIDATION_TYPE_PARTIAL, v, sizeof(VALIDATION_TYPE_PARTIAL)) == 0) {
        clioptions.enableCrc = false;
        return ROACH_VALIDATION_TYPE_PARTIAL;
    } else {
        return ROACH_VALIDATION_INVALID;
    }
}

/**
 * parseSnapshotAction
 * Parse for the snapshot action
 * @param value
 * @return
 */
SNAPSHOT_ACTION parseSnapshotAction(const char* value)
{
    const char* v = value;

    while (isspace(*v)) {
        v++;
    }

    if (strNCaseCmp(SNAPSHOT_BACKUP_PREPARE, v, sizeof(SNAPSHOT_BACKUP_PREPARE)) == 0) {
        return SNAPSHOT_ACTION_BACKUP_PREPARE;
    } else if (strNCaseCmp(SNAPSHOT_BACKUP_XLOG, v, sizeof(SNAPSHOT_BACKUP_XLOG)) == 0) {
        return SNAPSHOT_ACTION_BACKUP_XLOG;
    } else if (strNCaseCmp(SNAPSHOT_RESTORE_XLOG, v, sizeof(SNAPSHOT_RESTORE_XLOG)) == 0) {
        return SNAPSHOT_ACTION_RESTORE_XLOG;
    } else {
        return SNAPSHOT_ACTION_INVALID;
    }
}


/*
 * parseInt32
 * Parse string as 32bit signed int.
 * valid range: -2147483648 ~ 2147483647
 * @param value
 * @param result
 * @param uiLeastValue
 */
bool parseInt32(const char* value, int* result, int uiLeastValue, bool bCheckChar)
{
    int64_t val;
    char* endptr = NULL;

    if (roachStrCmp(value, INFINITE_STR) == 0) {
        *result = INT_MAX;
        return true;
    }

    errno = 0;
    val = strtol(value, &endptr, 0);
    if (val < uiLeastValue) {
        errno = ERANGE;
    }

    if (endptr == value || (bCheckChar && *endptr)) {
        return false;
    }

    if (errno == ERANGE || val != (int64_t)((int32_t)val)) {
        return false;
    }

    *result = (int32_t)val;

    return true;
}
/*
 * parseUInt32
 * Parse string as 32bit unsigned signed int.
 * valid range: 0 ~ 4294967295
 * @param value
 * @param result
 * @param uiLeastValue
 */
bool parseUInt32(const char* value, uint32_t* result, uint32_t uiLeastValue, bool bCheckChar)
{
    uint64_t val;
    char* endptr = NULL;

    if (roachStrCmp(value, INFINITE_STR) == 0) {
        *result = UINT_MAX;
        return true;
    }

    errno = 0;
    val = strtoul(value, &endptr, 0);
    if (val < uiLeastValue) {
        errno = ERANGE;
        return false;
    }
    if (endptr == value || (bCheckChar && *endptr)) {
        return false;
    }

    *result = (unsigned int)val;

    return true;
}

/*
 * parseUInt64
 * Parse string as 32bit unsigned signed int.
 * valid range: 0 ~ 4294967295
 * @param value
 * @param result
 * @param uiLeastValue
 */
bool parseUInt64(const char* value, uint64_t* result, uint64_t uiLeastValue, bool bCheckChar)
{
    uint64_t val;
    char* endptr = NULL;

    if (roachStrCmp(value, INFINITE_STR) == 0) {
        *result = UINT_MAX;
        return true;
    }

    errno = 0;
    val = strtoul(value, &endptr, 0);
    if (val < uiLeastValue) {
        errno = ERANGE;
        return false;
    }
    if (endptr == value || (bCheckChar && *endptr)) {
        return false;
    }

    *result = val;

    return true;
}

/**
* printMsgForInvalidValueSpecified
* @brief    Print error message
* @param    opt
* @param    optargs
* @param    message
* @return

* @exception    None
*/
void printMsgForInvalidValueSpecified(roach_option* opt, const char* optargs, const char* message)
{
    if (isprint(opt->sname)) {
        LOGERROR("option -%c, --%s should be %s: '%s'", opt->sname, opt->lname, message, optargs);
    } else {
        LOGERROR("option --%s should be %s: '%s'", opt->lname, message, optargs);
    }
}

ERROR_CODE FinalizeOptionValue(INSTANCE_OPERATIONS operation)
{
    ERROR_CODE ec = EC_SUCCESS;

    if (clioptions.bufblksize == 0) {
        if (clioptions.mediaType == ROACH_MEDIA_TYPE_OBS) {
            clioptions.bufblksize = DEFAULT_BUFFER_BLK_SZ_OBS;
        } else {
            clioptions.bufblksize = DEFAULT_BUFFER_BLK_SZ;
        }
    }

    if (clioptions.bufferSize == 0) {
        if (clioptions.mediaType == ROACH_MEDIA_TYPE_OBS) {
            clioptions.bufferSize = DEFAULT_BUFFER_SZ_OBS;
        } else {
            clioptions.bufferSize = DEFAULT_BUFFER_SZ;
        }
    }

    /* if not provided, use backup-key specific replication slot name for backup */
    if (clioptions.slotName == NULL && clioptions.backupKey != NULL) {
        int length;
        char * tmpSlotName = NULL;
        int nRet;

        if (operation == BACKUP && clioptions.priorBackupKey != NULL) {
            length = strlen(ROACH_INC_BAK_SLOT) + strlen("_") + strlen(clioptions.backupKey) + STRTERMINATIONLEN;
            tmpSlotName = (char *)MALLOC0(length);
            if (tmpSlotName == NULL) {
                LOGERROR("Failed to allocate memory for slot name");
                return EC_MEMALLOC_FAILURE;
            }

            nRet = snprintf_s(tmpSlotName, length, length - 1,
                        "%s_%s", ROACH_INC_BAK_SLOT, clioptions.backupKey);
            securec_check_ss_c(nRet, "", "");
        } else {
            length = strlen(ROACH_FULL_BAK_SLOT) + strlen("_") + strlen(clioptions.backupKey) + STRTERMINATIONLEN;
            tmpSlotName = (char *)MALLOC0(length);
            if (tmpSlotName == NULL) {
                LOGERROR("Failed to allocate memory for slot name");
                return EC_MEMALLOC_FAILURE;
            }

            nRet = snprintf_s(tmpSlotName, length, length - 1,
                        "%s_%s", ROACH_FULL_BAK_SLOT, clioptions.backupKey);
            securec_check_ss_c(nRet, "", "");
        }

        clioptions.slotName = tmpSlotName;
    }

    return ec;
}

/**
* getArgvValueForBufferOption
* @brief    Parse and assign the  buffer value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForBufferOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;

    do {
        ret = parseInt32(optargs, (int*)(opt->var), 0, true);

        // buffer size should be 256 to 16GB
        if (strCaseCmp(opt->lname, "buffer-size") == 0) {
            if (!ret || (*(int*)opt->var < MIN_BUFFER_SZ) || (*(int*)opt->var > MAX_BUFFER_SZ)) {
                message = "an integer (256 to 16384)";
                break;
            }
        }
        // buffer size should be 8192KB * clioptions.prefetchBlock(1-8192) to 256MB
        else if (strCaseCmp(opt->lname, "buffer-block-size") == 0) {
            if (!ret || (*(int*)opt->var < (MIN_BUFFER_BLK_SZ * clioptions.prefetchBlock)) ||
                (*(int*)opt->var > MAX_BUFFER_BLK_SZ)) {
                message = "an integer (8192 * prefetch-block(1-8192) to 268435456‬)";
                break;
            }
        } else /* if option doesnt matach the above */
        {
            return EC_OPTION_NOT_MATCH;
        }

        if (true == ret) {
            return EC_SUCCESS;
        }
    } while (false);

    printMsgForInvalidValueSpecified(opt, optargs, message);
    return EC_INVALID_OPTION;
}

/**
* getArgvValueForCpuOption
* @brief    Parse and assign the  cpu-relinquish-time/size value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForCpuOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;

    do {
        ret = parseInt32(optargs, (int*)(opt->var), 0, true);

        // "cpu relinquish time" should be in the range of  to
        if (strCaseCmp(opt->lname, "cpu-relinquish-time") == 0) {
            if (!ret || (*(int*)opt->var < MIN_CPU_RELINQUISH_TIME) || (*(int*)opt->var > MAX_CPU_RELINQUISH_TIME)) {
                message = "an integer (0 to 3600)";
                break;
            }
        }

        // "cpu relinquish size" should be in the range of  to
        else if (strCaseCmp(opt->lname, "cpu-relinquish-size") == 0) {
            if (!ret || (*(int*)opt->var < MIN_CPU_RELINQUISH_SIZE) || (*(int*)opt->var > MAX_CPU_RELINQUISH_SIZE)) {
                message = "an integer (1 to 10000)";
                break;
            }
        } else /* if option doesnt matach the above */
        {
            return EC_OPTION_NOT_MATCH;
        }

        if (true == ret) {
            return EC_SUCCESS;
        }
    } while (false);

    printMsgForInvalidValueSpecified(opt, optargs, message);
    return EC_INVALID_OPTION;
}

/**
* getArgvValueForRetryOption
* @brief    Parse and assign the retry value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForRetryOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;

    do {
        ret = parseInt32(optargs, (int*)(opt->var), 0, true);

        // failure retry count should be in the range of 0 to 256
        if (strCaseCmp(opt->lname, "failure-retry-count") == 0) {
            if (!ret || (*(int*)opt->var < MIN_FAIL_RETRY_COUNT) || (*(int*)opt->var > MAX_FAIL_RETRY_COUNT)) {
                message = "an integer (0 to 256)";
                break;
            }
        } else if (strCaseCmp(opt->lname, "resource-retry-count") == 0) {
            if (!ret || (*(int*)opt->var < MIN_RESOURCE_RETRY_COUNT) || (*(int*)opt->var > MAX_RESOURCE_RETRY_COUNT)) {
                message = "an integer (0 to 256)";
                break;
            }
        }

        // transaction commit time should be in the range of 60 to 60 * 60 * 10
        else if (strCaseCmp(opt->lname, "retry-wait-time") == 0) {
            if (!ret || (*(int*)opt->var < MIN_RETRY_WAIT_TIME) || (*(int*)opt->var > MAX_RETRY_WAIT_TIME)) {
                message = "an integer (1 to 3600)";
                break;
            }
        } else /* if option doesnt matach the above */
        {
            return EC_OPTION_NOT_MATCH;
        }

        if (true == ret) {
            return EC_SUCCESS;
        }
    } while (false);
    printMsgForInvalidValueSpecified(opt, optargs, message);
    return EC_INVALID_OPTION;
}

/**
 * getCLIOptionsMasterPortAgentPortCmp
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "compression level", and "parallel process" , and "cbmRecycle level"options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getCLIOptionsCompLvlParallelProcCmp(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;

    // compression level should be in the range of 0 to 9
    if (strCaseCmp(opt->lname, "compression-level") == 0) {
        if (!ret || (*(int*)opt->var < 0) || (*(int*)opt->var > MAX_COMPRESSION_LEVEL)) {
            *message = "an integer (0 to 9)";
            return EC_INVALID_OPTION;
        }
    }

    // parallel-process should be in the range of 1 to 128
    else if (strCaseCmp(opt->lname, "parallel-process") == 0) {
        if (!ret || (*(int*)opt->var < 1) || (*(int*)opt->var > MAX_PROCESS)) {
            *message = "an integer (1 to 32)";
            return EC_INVALID_OPTION;
        }
    } else {
        return EC_OPTION_NOT_MATCH;
    }

    return ec;
}

/**
 * getCLIOptionsMasterPortAgentPortCmp
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "master-port", and "agent-port" options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getCLIOptionsMasterPortAgentPortCmp(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;

    if (strCaseCmp(opt->lname, "master-port") == 0 || strCaseCmp(opt->lname, "agent-port") == 0) {
        if (!ret || (*(int*)opt->var < MIN_VALID_PORT) || (*(int*)opt->var > MAX_VALID_PORT)) {
            *message = "an integer (1024 to 65535)";
            return EC_INVALID_OPTION;
        }
    } else {
        ec = getCLIOptionsCompLvlParallelProcCmp(opt, message, ret);
        if (ec != EC_SUCCESS) {
            return ec;
        }
    }

    return ec;
}

/**
 * getArgvValueForCmnOptionMstPrtCmprParProc
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "master-port", "compression-level" ,"parallel-process" and cbm-recycle-level options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getArgvValueForCmnOptionMstPrtCmprParProc(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;
    UNUSED(message);

    ec = getCLIOptionsMasterPortAgentPortCmp(opt, message, ret);
    if (ec != EC_SUCCESS) {
        return ec;
    }

    return EC_SUCCESS;
}

/**
 * getCLIOptionsRestoreBufferThresholdCmp
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "Restore Buffer Threshold" options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getCLIOptionsRestoreBufferThresholdCmp(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;

    if (strCaseCmp(opt->lname, "restore-buffer-threshold") == 0) {
        if (!ret || (*(int*)opt->var < MIN_AVAIL_BUFF_PERCENT) || (*(int*)opt->var > MAX_AVAIL_BUFF_PERCENT)) {
            *message = "an integer (1 to 100)";
            return EC_INVALID_OPTION;
        }
    } else { /* if option doesnt matach the above */
        return EC_OPTION_NOT_MATCH;
    }

    return ec;
}

/**
 * getCLIOptionsFileSplitSizeCmp
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "File Split size" options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getCLIOptionsFileSplitSizeCmp(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;

    if (strCaseCmp(opt->lname, "filesplit-size") == 0) {
        if (!ret || (*(int*)opt->var < MIN_SPLIT_FILE_SIZE) || (*(int*)opt->var > MAX_SPLIT_FILE_SIZE) ||
            ((*(int*)opt->var) % SPLIT_FILE_MULTIPLE_OF_SIZE != 0)) {
            *message = "an integer (0 to 1024, multiple of 4)";
            return EC_INVALID_OPTION;
        }
    } else {
        ec = getCLIOptionsRestoreBufferThresholdCmp(opt, message, ret);
        if (ec != EC_SUCCESS) {
            return ec;
        }
    }

    return ec;
}

/**
 * getCLIOptionsAfterThresholdCmp
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 * to check "After Threshold" options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getCLIOptionsAfterThresholdCmp(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;

    // "getdata-waittime-afterthreshold" should be in the range of  to
    if (strCaseCmp(opt->lname, "getdata-waittime-afterthreshold") == 0) {
        if (!ret || (*(int*)opt->var < MIN_GETDATA_WAITTIME_AFTERTHRESHOLD) ||
            (*(int*)opt->var > MAX_GETDATA_WAITTIME_AFTERTHRESHOLD)) {
            *message = "an integer (0 to 1800000000)";
            return EC_INVALID_OPTION;
        }
    } else {
        ec = getCLIOptionsFileSplitSizeCmp(opt, message, ret);
        if (ec != EC_SUCCESS) {
            return ec;
        }
    }

    return ec;
}

/**
 * getArgvValueForCmnOptionThrshldFileSplt
 * @brief    Code chunk derived from splitting up "getArgvValueForCmnOption"
 *           to check "getdata-waittime-afterthreshold", "filesplit-size" and
 *           "restore-buffer-threshold" options
 * @param    opt     : Option struct pointer
 * @param    message : String to hold hint message
 * @param    ret     : boolean retval from "parse" function
 * @return   success/failure
 */
ERROR_CODE getArgvValueForCmnOptionThrshldFileSplt(roach_option* opt, const char** message, bool ret)
{
    ERROR_CODE ec = EC_SUCCESS;
    UNUSED(message);

    ec = getCLIOptionsAfterThresholdCmp(opt, message, ret);
    if (ec != EC_SUCCESS) {
        return ec;
    }

    return EC_SUCCESS;
}

/**
* getArgvValueForCmnOption
* @brief    Parse and assign the value for option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForCmnOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;
    ERROR_CODE ulRet = EC_BUTT;

    ret = parseInt32(optargs, (int*)(opt->var), 0, true);
    do {
        ulRet = getArgvValueForCmnOptionMstPrtCmprParProc(opt, &message, ret);
        if (ulRet == EC_INVALID_OPTION) {
            break;
        } else if (ulRet != EC_SUCCESS) {
            ulRet = getArgvValueForCmnOptionThrshldFileSplt(opt, &message, ret);
            if (ulRet == EC_INVALID_OPTION) {
                break;
            } else if (ulRet != EC_SUCCESS) {
                return EC_OPTION_NOT_MATCH;
            }
        }
        if (true == ret) {
            return EC_SUCCESS;
        }
    } while (false);
    printMsgForInvalidValueSpecified(opt, optargs, message);

    return EC_INVALID_OPTION;
}

/**
* getArgvValueForLogOption
* @brief    Parse and assign the log value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForLogOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;

    do {
        ret = parseInt32(optargs, (int*)(opt->var), 0, true);

        // log filesize should be in the range of 5 to 20
        if (strCaseCmp(opt->lname, "log-filesize") == 0) {
            if (!ret || (*(int*)opt->var < MIN_LOGFILE_SZ) || (*(int*)opt->var > MAX_LOGFILE_SZ)) {
                message = "an integer (5 to 20)";
                break;
            }
        }

        // log filecount should be in the range of 5 to 1024
        else if (strCaseCmp(opt->lname, "log-filecount") == 0) {
            if (!ret || (*(int*)opt->var < MIN_LOGFILE_CNT) || (*(int*)opt->var > MAX_LOGFILE_CNT)) {
                message = "an integer (5 to 1024)";
                break;
            }
        } else /* if option doesnt matach the above */
        {
            return EC_OPTION_NOT_MATCH;
        }

        if (true == ret) {
            return EC_SUCCESS;
        }
    } while (false);

    printMsgForInvalidValueSpecified(opt, optargs, message);
    return EC_INVALID_OPTION;
}

/**
* getArgvValueForIntOption
* @brief    Parse and assign the integer value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForIntOption(roach_option* opt, const char* optargs)
{
    const char* message = NULL;
    bool ret = true;
    ERROR_CODE ec = EC_SUCCESS;

    ret = parseInt32(optargs, (int*)(opt->var), 0, true);
    if (false == ret) {
        message = "an integer (1 to 2147483647)";
        printMsgForInvalidValueSpecified(opt, optargs, message);
        return EC_INVALID_OPTION;
    }

    /* get value for common interger options */
    ec = getArgvValueForCmnOption(opt, optargs);
    if (ec != EC_OPTION_NOT_MATCH) {
        return ec;
    }

    /* get value for all buferr options */
    ec = getArgvValueForBufferOption(opt, optargs);
    if (ec != EC_OPTION_NOT_MATCH) {
        return ec;
    }

    /* get value for cpu relinquish options */
    ec = getArgvValueForCpuOption(opt, optargs);
    if (ec != EC_OPTION_NOT_MATCH) {
        return ec;
    }

    /* get value for retry options */
    ec = getArgvValueForRetryOption(opt, optargs);
    if (ec != EC_OPTION_NOT_MATCH) {
        return ec;
    }

    /* get value for log options */
    ec = getArgvValueForLogOption(opt, optargs);
    if (ec != EC_OPTION_NOT_MATCH) {
        return ec;
    }

    return EC_SUCCESS;
}

/**
* getArgvValueForStringOption
* @brief    Assign the string value for the option
* @param    opt
* @param    optargs
* @return

* @exception    None
*/
ERROR_CODE getArgvValueForStringOption(roach_option* opt, const char* optargs)
{
    if (strlen(optargs) == 0) {
        LOGERROR("Invalid value specified for %s", opt->lname);
        return EC_INVALID_ARGUMENT;
    }

    // Check logging level
    if (strNCaseCmp(opt->lname, "logging-level", strlen("logging-level")) == 0) {
        clioptions.loggingLevel = parseLoggingLevel(optargs);
        if (clioptions.loggingLevel == ROACH_LOG_NONE) {
            LOGERROR("Invalid logging level specified");

            return EC_INVALID_ARGUMENT;
        }

        return EC_SUCCESS;
    }
    // Check Backup Type
    else if (strNCaseCmp(opt->lname, "backup-type", strlen("backup-type")) == 0) {
        clioptions.eBackupType = parseBackupType(optargs);
        if (clioptions.eBackupType == BACKUP_MODE_INVALID) {
            LOGERROR("Invalid backup type specified");
            return EC_INVALID_ARGUMENT;
        }
        return EC_SUCCESS;
    }
    /* Check Media Type */
    else if (strNCaseCmp(opt->lname, "media-type", strlen("media-type")) == 0) {
        clioptions.mediaType = parseMediaType(optargs);
        if (clioptions.mediaType == ROACH_MEDIA_INVALID) {
            LOGERROR("Invalid media type specified");

            return EC_INVALID_ARGUMENT;
        }

        return EC_SUCCESS;
    }
    /* Check Validation Type */
    else if (strNCaseCmp(opt->lname, "validation-type", strlen("validation-type")) == 0) {
        clioptions.validationType = parseValidationType(optargs);
        if (clioptions.validationType == ROACH_VALIDATION_INVALID) {
            LOGERROR("Invalid Validation Type Specified\n");

            return EC_INVALID_ARGUMENT;
        }
        return EC_SUCCESS;
    }
    /* Check snapshot action */
    else if (strNCaseCmp(opt->lname, "snapshot-action", strlen("snapshot-action")) == 0) {
        clioptions.snapshotAction = parseSnapshotAction(optargs);
        if (clioptions.snapshotAction == SNAPSHOT_ACTION_INVALID) {
            LOGERROR("Invalid Snapshot Action Specified\n");

            return EC_INVALID_ARGUMENT;
        }
        return EC_SUCCESS;
    }

    /* Check Tablename only in Master machine */
    if (!clioptions.bMaster && strCaseCmp(opt->lname, "tablename") == 0) {
        return EC_SUCCESS;
    }
    if (opt->source != SOURCE_DEFAULT) {
        FREE(*(char**)opt->var);
    }

    *(char**)opt->var = STRDUP(optargs);
    if (*(char**)opt->var == NULL) {
        LOGERROR("String duplication failed when get string option value");
        return EC_MEMALLOC_FAILURE;
    }

    return EC_SUCCESS;
}

/* compare two strings ignore cases and ignore -_ */
bool key_equals(const char* lhs, const char* rhs)
{
    // Check the string
    for (; *lhs && *rhs; lhs++, rhs++) {
        if (strchr("-_ ", *lhs)) {
            if (!strchr("-_ ", *rhs)) {
                return false;
            }
        } else if (tolower(*lhs) != tolower(*rhs)) { // Check case insensitive
            return false;
        }
    }

    return *lhs == '\0' && *rhs == '\0';  // all characters matching
}

/**
* @brief    assign the value to the passed option
* @param    opt
* @param    optarg
* @param    src
* @return

* @exception    None

* @date     2015/3/9
*/
ERROR_CODE getArgvValueForOption(roach_option* opt, const char* optargs, pgut_optsrc src)
{
    const char* message = NULL;
    ERROR_CODE ulRet = EC_SUCCESS;
    // if opt not specified
    if (opt == NULL) {
        LOGERROR("Invalid option provided");
        return EC_INVALID_OPTION;
    }

    if (opt->source > src) {
        /* high prior value has been set already. */
        return EC_SUCCESS;
    } else {
        /* can be overwritten if non-command line source */
        opt->source = src;

        switch (opt->type) {
            /* get string arguments
             */
            case 's':
                return getArgvValueForStringOption(opt, optargs);

            /* get boolean arguments
             */
            case 'b':
                ulRet = getBooleanArguments(opt, optargs);
                if (ulRet != EC_SUCCESS) {
                    message = "a boolean";
                    break;
                }

                return EC_SUCCESS;

            /* get integer arguments
             */
            case 'i':
                return getArgvValueForIntOption(opt, optargs);

                /*
                 * For no password option
                 */

            case 'Y':
                ulRet = getArgvValueForNoPassword(opt, optargs);
                if (ulRet != EC_SUCCESS) {
                    message = "a boolean";
                    break;
                }

                return EC_SUCCESS;

            /*
             * For password option
             */
            case 'y':
                return getBooleanArguments(opt, optargs);

            /* Invalid option type
             */
            default:
                LOGERROR("invalid option type: %c", opt->type);
                return EC_INVALID_OPTION; /* keep compiler quiet */
        }
    }

    /* Print the error message
     */
    printMsgForInvalidValueSpecified(opt, optargs, message);
    return EC_INVALID_OPTION;
}

ERROR_CODE getBooleanArguments(roach_option* opt, const char* optargs)
{
    if (optargs != NULL) {
        if (parseBool(optargs, (bool*)opt->var)) {
            return EC_SUCCESS;
        }
    } else {
        *((bool*)opt->var) = true;
        return EC_SUCCESS;
    }

    return EC_INVALID_OPTION;
}

ERROR_CODE getArgvValueForNoPassword(roach_option* opt, const char* optargs)
{
    if (optargs == NULL) {
        *(YesNo*)opt->var = (opt->type == 'y' ? YES : NO);
        return EC_SUCCESS;
    } else {
        bool value = false;
        if (parseBool(optargs, &value)) {
            *(YesNo*)opt->var = (value ? YES : NO);
            return EC_SUCCESS;
        }
    }

    return EC_INVALID_OPTION;
}

/**
* skip_space
* @brief    skip the space from the input string
* @param    str
* @param    line
* @return

* @exception    None

* @date     2015/3/9
*/
const char* skip_space(const char* str)
{
    while (isspace(*str)) {
        str++;
    }

    return str;
}

/**
* get_next_token
* @brief    get the token from the string
* @param    src
* @param    dst
* @param    line
* @return

* @exception    None

* @date     2015/3/9
*/
const char* get_next_token(const char* src, char* dst, size_t dstLen, const char* line)
{
    const char* s = NULL;
    size_t i;
    size_t j;
    int nRet = 0;

    if (*(s = skip_space(src)) == '\0') {
        return NULL;
    }

    /*
     * parse quoted string
     */
    if (*s == '\'') {
        s++;
        for (i = 0, j = 0; s[i] != '\0'; i++) {
            /* Return NULL if dst length is insufficient. Reserve 1 byte for '\0' */
            if (j + 1 >= dstLen) {
                return NULL;
            }
            if (s[i] == '\\') {
                i++;
                parse_next_token(s, dst, i, j);
            } else if (s[i] == '\'') {
                i++;

                /* doubled quote becomes just one quote */
                if (s[i] == '\'') {
                    dst[j] = s[i];
                } else {
                    break;
                }
            } else {
                dst[j] = s[i];
            }

            j++;
        }
    } else {
        i = j = strcspn(s, "# \n\r\t\v");
        nRet = memcpy_s(dst, dstLen - 1, s, j);
        securec_check_c(nRet, "\0", "\0");
    }

    dst[j] = '\0';
    return s + i;
}
