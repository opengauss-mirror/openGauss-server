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
 * alarm.cpp
 *    openGauss alarm reporting/logging definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/lib/alarm/alarm.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <time.h>
#include "common/config/cm_config.h"

#include "alarm/alarm.h"
#include "syslog.h"
#include "securec.h"
#include "securec_check.h"
#include "alarm/alarm_log.h"

#ifdef ENABLE_UT
#define static
#endif

static char MyHostName[CM_NODE_NAME] = {0};
static char MyHostIP[CM_NODE_NAME] = {0};
static char WarningType[CM_NODE_NAME] = {0};
static char ClusterName[CLUSTER_NAME_LEN] = {0};
static char LogicClusterName[CLUSTER_NAME_LEN] = {0};
// declare the guc variable of alarm module
char* Alarm_component = NULL;
THR_LOCAL int AlarmReportInterval = 10;

static int  IP_LEN = 64;  /* default ip len */
static int  AF_INET6_MAX_BITS = 128;  /* ip mask bit */
// if report alarm succeed(component), return 0
#define ALARM_REPORT_SUCCEED 0
// if report alarm suppress(component), return 2
#define ALARM_REPORT_SUPPRESS 2
#define CLUSTERNAME "MPP_CLUSTER"
#define FUSIONINSIGHTTYPE "1"
#define ICBCTYPE "2"
#define CBGTYPE "5"
#define ALARMITEMNUMBER 64
#define ALARM_LOGEXIT(ErrMsg, fp)        \
    do {                                 \
        AlarmLog(ALM_LOG, "%s", ErrMsg); \
        if ((fp) != NULL)                \
            fclose(fp);                  \
        return;                          \
    } while (0)

// call back function for get logic cluster name
static cb_for_getlc cb_GetLCName = NULL;

static AlarmName AlarmNameMap[ALARMITEMNUMBER];

static char* AlarmIdToAlarmNameEn(AlarmId id);
static char* AlarmIdToAlarmNameCh(AlarmId id);
static char* AlarmIdToAlarmInfoEn(AlarmId id);
static char* AlarmIdToAlarmInfoCh(AlarmId id);
static char* AlarmIdToAlarmLevel(AlarmId id);
static void ReadAlarmItem(void);
static void GetHostName(char* myHostName, unsigned int myHostNameLen);
static void GetHostIP(const char* myHostName, char* myHostIP, unsigned int myHostIPLen, bool enableLogHostname);
static void GetClusterName(char* clusterName, unsigned int clusterNameLen);
static bool CheckAlarmComponent(const char* alarmComponentPath);
static bool SuppressComponentAlarmReport(Alarm* alarmItem, AlarmType type, int timeInterval);
static bool SuppressSyslogAlarmReport(Alarm* alarmItem, AlarmType type, int timeInterval);
static void ComponentReport(
    char* alarmComponentPath, Alarm* alarmItem, AlarmType type, AlarmAdditionalParam* additionalParam);
static void SyslogReport(Alarm* alarmItem, AlarmAdditionalParam* additionalParam);
static void check_input_for_security1(char* input);
static void AlarmScopeInitialize(void);
void AlarmReporter(Alarm* alarmItem, AlarmType type, AlarmAdditionalParam* additionalParam);
void AlarmLog(int level, const char* fmt, ...);

/*
 * @Description: check input for security
 * @IN input: input string
 * @Return:  void
 * @See also:
 */
static void check_input_for_security1(char* input)
{
    char* danger_token[] = {"|",
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

    for (int i = 0; danger_token[i] != NULL; ++i) {
        if (strstr(input, danger_token[i]) != NULL) {
            printf("invalid token \"%s\"\n", danger_token[i]);
            exit(1);
        }
    }
}

static char* AlarmIdToAlarmNameEn(AlarmId id)
{
    unsigned int i;
    for (i = 0; i < sizeof(AlarmNameMap) / sizeof(AlarmName); ++i) {
        if (id == AlarmNameMap[i].id)
            return AlarmNameMap[i].nameEn;
    }
    return "unknown";
}

static char* AlarmIdToAlarmNameCh(AlarmId id)
{
    unsigned int i;
    for (i = 0; i < sizeof(AlarmNameMap) / sizeof(AlarmName); ++i) {
        if (id == AlarmNameMap[i].id)
            return AlarmNameMap[i].nameCh;
    }
    return "unknown";
}

static char* AlarmIdToAlarmInfoEn(AlarmId id)
{
    unsigned int i;
    for (i = 0; i < sizeof(AlarmNameMap) / sizeof(AlarmName); ++i) {
        if (id == AlarmNameMap[i].id)
            return AlarmNameMap[i].alarmInfoEn;
    }
    return "unknown";
}

static char* AlarmIdToAlarmInfoCh(AlarmId id)
{
    unsigned int i;
    for (i = 0; i < sizeof(AlarmNameMap) / sizeof(AlarmName); ++i) {
        if (id == AlarmNameMap[i].id)
            return AlarmNameMap[i].alarmInfoCh;
    }
    return "unknown";
}

static char* AlarmIdToAlarmLevel(AlarmId id)
{
    unsigned int i;
    for (i = 0; i < sizeof(AlarmNameMap) / sizeof(AlarmName); ++i) {
        if (id == AlarmNameMap[i].id)
            return AlarmNameMap[i].alarmLevel;
    }
    return "unknown";
}

static void ReadAlarmItem(void)
{
    const int MAX_ERROR_MSG = 128;
    char* gaussHomeDir = NULL;
    char alarmItemPath[MAXPGPATH];
    char Lrealpath[MAXPGPATH * 4] = {0};
    char* realPathPtr = NULL;
    char* endptr = NULL;
    int alarmItemIndex;
    int nRet = 0;
    char tempStr[MAXPGPATH];
    char* subStr1 = NULL;
    char* subStr2 = NULL;
    char* subStr3 = NULL;
    char* subStr4 = NULL;
    char* subStr5 = NULL;
    char* subStr6 = NULL;

    char* savePtr1 = NULL;
    char* savePtr2 = NULL;
    char* savePtr3 = NULL;
    char* savePtr4 = NULL;
    char* savePtr5 = NULL;
    char* savePtr6 = NULL;

    errno_t rc = 0;
    size_t len = 0;

    char ErrMsg[MAX_ERROR_MSG];

    gaussHomeDir = gs_getenv_r("GAUSSHOME");
    if (gaussHomeDir == NULL) {
        AlarmLog(ALM_LOG, "ERROR: environment variable $GAUSSHOME is not set!\n");
        return;
    }
    check_input_for_security1(gaussHomeDir);

    nRet = snprintf_s(alarmItemPath, MAXPGPATH, MAXPGPATH - 1, "%s/bin/alarmItem.conf", gaussHomeDir);
    securec_check_ss_c(nRet, "\0", "\0");

    realPathPtr = realpath(alarmItemPath, Lrealpath);
    if (NULL == realPathPtr) {
        AlarmLog(ALM_LOG, "Get real path of alarmItem.conf failed!\n");
        return;
    }

    FILE* fp = fopen(Lrealpath, "r");
    if (NULL == fp) {
        ALARM_LOGEXIT("AlarmItem file is not exist!\n", fp);
    }

    rc = memset_s(ErrMsg, MAX_ERROR_MSG, 0, MAX_ERROR_MSG);
    securec_check_c(rc, "\0", "\0");

    for (alarmItemIndex = 0; alarmItemIndex < ALARMITEMNUMBER; ++alarmItemIndex) {
        if (NULL == fgets(tempStr, MAXPGPATH - 1, fp)) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Get line in AlarmItem file failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr1 = strtok_r(tempStr, "\t", &savePtr1);
        if (NULL == subStr1) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm ID failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr2 = strtok_r(savePtr1, "\t", &savePtr2);
        if (NULL == subStr2) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm English name failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr3 = strtok_r(savePtr2, "\t", &savePtr3);
        if (NULL == subStr3) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm Chinese name failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr4 = strtok_r(savePtr3, "\t", &savePtr4);
        if (NULL == subStr4) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm English info failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr5 = strtok_r(savePtr4, "\t", &savePtr5);
        if (NULL == subStr5) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm Chinese info failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }
        subStr6 = strtok_r(savePtr5, "\t", &savePtr6);
        if (subStr6 == NULL) {
            nRet = snprintf_s(ErrMsg,
                MAX_ERROR_MSG,
                MAX_ERROR_MSG - 1,
                "Invalid data in AlarmItem file! Read alarm Level info failed! line: %d\n",
                alarmItemIndex + 1);
            securec_check_ss_c(nRet, "\0", "\0");
            ALARM_LOGEXIT(ErrMsg, fp);
        }

        // get alarm ID
        errno = 0;
        AlarmNameMap[alarmItemIndex].id = (AlarmId)(strtol(subStr1, &endptr, 10));
        if ((endptr != NULL && *endptr != '\0') || errno == ERANGE) {
            ALARM_LOGEXIT("Get alarm ID failed!\n", fp);
        }

        // get alarm EN name
        len = (strlen(subStr2) < (sizeof(AlarmNameMap[alarmItemIndex].nameEn) - 1))
                  ? strlen(subStr2)
                  : (sizeof(AlarmNameMap[alarmItemIndex].nameEn) - 1);
        rc = memcpy_s(AlarmNameMap[alarmItemIndex].nameEn, sizeof(AlarmNameMap[alarmItemIndex].nameEn), subStr2, len);
        securec_check_c(rc, "\0", "\0");
        AlarmNameMap[alarmItemIndex].nameEn[len] = '\0';

        // get alarm CH name
        len = (strlen(subStr3) < (sizeof(AlarmNameMap[alarmItemIndex].nameCh) - 1))
                  ? strlen(subStr3)
                  : (sizeof(AlarmNameMap[alarmItemIndex].nameCh) - 1);
        rc = memcpy_s(AlarmNameMap[alarmItemIndex].nameCh, sizeof(AlarmNameMap[alarmItemIndex].nameCh), subStr3, len);
        securec_check_c(rc, "\0", "\0");
        AlarmNameMap[alarmItemIndex].nameCh[len] = '\0';

        // get alarm EN info
        len = (strlen(subStr4) < (sizeof(AlarmNameMap[alarmItemIndex].alarmInfoEn) - 1))
                  ? strlen(subStr4)
                  : (sizeof(AlarmNameMap[alarmItemIndex].alarmInfoEn) - 1);
        rc = memcpy_s(
            AlarmNameMap[alarmItemIndex].alarmInfoEn, sizeof(AlarmNameMap[alarmItemIndex].alarmInfoEn), subStr4, len);
        securec_check_c(rc, "\0", "\0");
        AlarmNameMap[alarmItemIndex].alarmInfoEn[len] = '\0';

        // get alarm CH info
        len = (strlen(subStr5) < (sizeof(AlarmNameMap[alarmItemIndex].alarmInfoCh) - 1))
                  ? strlen(subStr5)
                  : (sizeof(AlarmNameMap[alarmItemIndex].alarmInfoCh) - 1);
        rc = memcpy_s(
            AlarmNameMap[alarmItemIndex].alarmInfoCh, sizeof(AlarmNameMap[alarmItemIndex].alarmInfoCh), subStr5, len);
        securec_check_c(rc, "\0", "\0");
        AlarmNameMap[alarmItemIndex].alarmInfoCh[len] = '\0';

        /* get alarm LEVEL info */
        len = (strlen(subStr6) < (sizeof(AlarmNameMap[alarmItemIndex].alarmLevel) - 1))
                  ? strlen(subStr6)
                  : (sizeof(AlarmNameMap[alarmItemIndex].alarmLevel) - 1);
        rc = memcpy_s(
            AlarmNameMap[alarmItemIndex].alarmLevel, sizeof(AlarmNameMap[alarmItemIndex].alarmLevel), subStr6, len);
        securec_check_c(rc, "\0", "\0");
        /* alarm level is the last one in alarmItem.conf, we should delete line break */
        AlarmNameMap[alarmItemIndex].alarmLevel[len - 1] = '\0';
    }
    fclose(fp);
}

static void GetHostName(char* myHostName, unsigned int myHostNameLen)
{
    char hostName[CM_NODE_NAME];
    errno_t rc = 0;
    size_t len;

    (void)gethostname(hostName, CM_NODE_NAME);
    len = (strlen(hostName) < (myHostNameLen - 1)) ? strlen(hostName) : (myHostNameLen - 1);
    rc = memcpy_s(myHostName, myHostNameLen, hostName, len);
    securec_check_c(rc, "\0", "\0");
    myHostName[len] = '\0';
    AlarmLog(ALM_LOG, "Host Name: %s \n", myHostName);
}

static void GetHostIP(const char* myHostName, char* myHostIP, unsigned int myHostIPLen, bool enableLogHostname)
{
    struct hostent* hp;
    errno_t rc = 0;
    char* ipstr = NULL;
    char ipv6[IP_LEN] = {0};
    char* result = NULL;
    size_t len = 0;

    if (!enableLogHostname) {
        size_t myHostNameLen = strlen(myHostName);
        len = (myHostNameLen < (myHostIPLen - 1)) ? myHostNameLen : (myHostIPLen - 1);
        rc = memcpy_s(myHostIP, myHostIPLen, myHostName, len);
        securec_check_c(rc, "\0", "\0");
        myHostIP[len] = '\0';
        AlarmLog(ALM_LOG, "Host IP: %s. Copy hostname directly in case of taking 10s to use 'gethostbyname' when "
            "/etc/hosts does not contain <HOST IP>\n", myHostIP);
        return;
    }

    hp = gethostbyname(myHostName);
    if (hp == NULL) {
        hp = gethostbyname2(myHostName, AF_INET6);
        if (hp == NULL) {
            AlarmLog(ALM_LOG, "GET host IP by name failed.\n");
            return;
        }
    }

    if (hp->h_addrtype == AF_INET) {
        ipstr = inet_ntoa(*((struct in_addr*)hp->h_addr));
    } else if (hp->h_addrtype == AF_INET6) {
        result = inet_net_ntop(AF_INET6, ((struct in6_addr*)hp->h_addr), AF_INET6_MAX_BITS, ipv6, IP_LEN);
        if (result == NULL) {
            AlarmLog(ALM_LOG, "inet_net_ntop failed, error: %d.\n", EAFNOSUPPORT);
        }
        ipstr = ipv6;
    }
    len = (strlen(ipstr) < (myHostIPLen - 1)) ? strlen(ipstr) : (myHostIPLen - 1);
    rc = memcpy_s(myHostIP, myHostIPLen, ipstr, len);
    securec_check_c(rc, "\0", "\0");
    myHostIP[len] = '\0';
    AlarmLog(ALM_LOG, "Host IP: %s \n", myHostIP);
}

static void GetClusterName(char* clusterName, unsigned int clusterNameLen)
{
    errno_t rc = 0;
    char* gsClusterName = gs_getenv_r("GS_CLUSTER_NAME");

    if (gsClusterName != NULL) {
        check_input_for_security1(gsClusterName);
        size_t len = (strlen(gsClusterName) < (clusterNameLen - 1)) ? strlen(gsClusterName) : (clusterNameLen - 1);
        rc = memcpy_s(clusterName, clusterNameLen, gsClusterName, len);
        securec_check_c(rc, "\0", "\0");
        clusterName[len] = '\0';
        AlarmLog(ALM_LOG, "Cluster Name: %s \n", clusterName);
    } else {
        size_t len = strlen(CLUSTERNAME);
        rc = memcpy_s(clusterName, clusterNameLen, CLUSTERNAME, len);
        securec_check_c(rc, "\0", "\0");
        clusterName[len] = '\0';
        AlarmLog(ALM_LOG, "Get ENV GS_CLUSTER_NAME failed!\n");
    }
}

void AlarmEnvInitialize(bool enableLogHostname)
{
    char* warningType = NULL;
    int nRet = 0;
    warningType = gs_getenv_r("GAUSS_WARNING_TYPE");
    if ((NULL == warningType) || ('\0' == warningType[0])) {
        AlarmLog(ALM_LOG, "can not read GAUSS_WARNING_TYPE env.\n");
    } else {
        check_input_for_security1(warningType);
        // save warningType into WarningType array
        // WarningType is a static global variable
        nRet = snprintf_s(WarningType, sizeof(WarningType), sizeof(WarningType) - 1, "%s", warningType);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    // save this host name into MyHostName array
    // MyHostName is a static global variable
    GetHostName(MyHostName, sizeof(MyHostName));

    // save this host IP into MyHostIP array
    // MyHostIP is a static global variable
    GetHostIP(MyHostName, MyHostIP, sizeof(MyHostIP), enableLogHostname);

    // save this cluster name into ClusterName array
    // ClusterName is a static global variable
    GetClusterName(ClusterName, sizeof(ClusterName));

    // read alarm item info from the configure file(alarmItem.conf)
    ReadAlarmItem();

    // read alarm scope info from the configure file(alarmItem.conf)
    AlarmScopeInitialize();
}

/*
Fill in the structure AlarmAdditionalParam with alarmItem has been filled.
*/
static void FillAlarmAdditionalInfo(AlarmAdditionalParam* additionalParam, const char* instanceName,
    const char* databaseName, const char* dbUserName, const char* logicClusterName, Alarm* alarmItem)
{
    errno_t rc = 0;
    int nRet = 0;
    int lenAdditionInfo = sizeof(additionalParam->additionInfo);

    // fill in the addition Info field
    nRet = snprintf_s(additionalParam->additionInfo, lenAdditionInfo, lenAdditionInfo - 1, "%s", alarmItem->infoEn);
    securec_check_ss_c(nRet, "\0", "\0");

    // fill in the cluster name field
    size_t lenClusterName = strlen(ClusterName);
    rc = memcpy_s(additionalParam->clusterName, sizeof(additionalParam->clusterName) - 1, ClusterName, lenClusterName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->clusterName[lenClusterName] = '\0';

    // fill in the host IP field
    size_t lenHostIP = strlen(MyHostIP);
    rc = memcpy_s(additionalParam->hostIP, sizeof(additionalParam->hostIP) - 1, MyHostIP, lenHostIP);
    securec_check_c(rc, "\0", "\0");
    additionalParam->hostIP[lenHostIP] = '\0';

    // fill in the host name field
    size_t lenHostName = strlen(MyHostName);
    rc = memcpy_s(additionalParam->hostName, sizeof(additionalParam->hostName) - 1, MyHostName, lenHostName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->hostName[lenHostName] = '\0';

    // fill in the instance name field
    size_t lenInstanceName = (strlen(instanceName) < (sizeof(additionalParam->instanceName) - 1))
                                 ? strlen(instanceName)
                                 : (sizeof(additionalParam->instanceName) - 1);
    rc = memcpy_s(
        additionalParam->instanceName, sizeof(additionalParam->instanceName) - 1, instanceName, lenInstanceName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->instanceName[lenInstanceName] = '\0';

    // fill in the database name field
    size_t lenDatabaseName = (strlen(databaseName) < (sizeof(additionalParam->databaseName) - 1))
                                 ? strlen(databaseName)
                                 : (sizeof(additionalParam->databaseName) - 1);
    rc = memcpy_s(
        additionalParam->databaseName, sizeof(additionalParam->databaseName) - 1, databaseName, lenDatabaseName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->databaseName[lenDatabaseName] = '\0';

    // fill in the dbuser name field
    size_t lenDbUserName = (strlen(dbUserName) < (sizeof(additionalParam->dbUserName) - 1))
                               ? strlen(dbUserName)
                               : (sizeof(additionalParam->dbUserName) - 1);
    rc = memcpy_s(additionalParam->dbUserName, sizeof(additionalParam->dbUserName) - 1, dbUserName, lenDbUserName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->dbUserName[lenDbUserName] = '\0';

    if (logicClusterName == NULL)
        return;

    // fill in the logic cluster name field
    size_t lenLogicClusterName = strlen(logicClusterName);
    size_t bufLen = sizeof(additionalParam->logicClusterName) - 1;
    if (lenLogicClusterName > bufLen)
        lenLogicClusterName = bufLen;

    rc = memcpy_s(additionalParam->logicClusterName, bufLen, logicClusterName, lenLogicClusterName);
    securec_check_c(rc, "\0", "\0");
    additionalParam->logicClusterName[lenLogicClusterName] = '\0';
}

void SetcbForGetLCName(cb_for_getlc get_lc_name)
{
    cb_GetLCName = get_lc_name;
}

static char* GetLogicClusterName()
{
    if (NULL != cb_GetLCName)
        cb_GetLCName(LogicClusterName);
    return LogicClusterName;
}

/*
Fill in the structure AlarmAdditionalParam
*/
void WriteAlarmAdditionalInfo(AlarmAdditionalParam* additionalParam, const char* instanceName, const char* databaseName,
    const char* dbUserName, Alarm* alarmItem, AlarmType type, ...)
{
    errno_t rc;
    int nRet;
    int lenInfoEn = sizeof(alarmItem->infoEn);
    int lenInfoCh = sizeof(alarmItem->infoCh);
    va_list argp1;
    va_list argp2;
    char* logicClusterName = NULL;

    // initialize the additionalParam
    rc = memset_s(additionalParam, sizeof(AlarmAdditionalParam), 0, sizeof(AlarmAdditionalParam));
    securec_check_c(rc, "\0", "\0");
    // initialize the alarmItem->infoEn
    rc = memset_s(alarmItem->infoEn, lenInfoEn, 0, lenInfoEn);
    securec_check_c(rc, "\0", "\0");
    // initialize the alarmItem->infoCh
    rc = memset_s(alarmItem->infoCh, lenInfoCh, 0, lenInfoCh);
    securec_check_c(rc, "\0", "\0");

    if (ALM_AT_Fault == type || ALM_AT_Event == type) {
        va_start(argp1, type);
        va_start(argp2, type);
        nRet = vsnprintf_s(alarmItem->infoEn, lenInfoEn, lenInfoEn - 1, AlarmIdToAlarmInfoEn(alarmItem->id), argp1);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = vsnprintf_s(alarmItem->infoCh, lenInfoCh, lenInfoCh - 1, AlarmIdToAlarmInfoCh(alarmItem->id), argp2);
        securec_check_ss_c(nRet, "\0", "\0");
        va_end(argp1);
        va_end(argp2);
    }

    logicClusterName = GetLogicClusterName();
    FillAlarmAdditionalInfo(additionalParam, instanceName, databaseName, dbUserName, logicClusterName, alarmItem);
}

/*
Fill in the structure AlarmAdditionalParam for logic cluster.
*/
void WriteAlarmAdditionalInfoForLC(AlarmAdditionalParam* additionalParam, const char* instanceName,
    const char* databaseName, const char* dbUserName, const char* logicClusterName, Alarm* alarmItem, AlarmType type,
    ...)
{
    errno_t rc = 0;
    int nRet = 0;
    int lenInfoEn = sizeof(alarmItem->infoEn);
    int lenInfoCh = sizeof(alarmItem->infoCh);
    va_list argp1;
    va_list argp2;

    // initialize the additionalParam
    rc = memset_s(additionalParam, sizeof(AlarmAdditionalParam), 0, sizeof(AlarmAdditionalParam));
    securec_check_c(rc, "\0", "\0");
    // initialize the alarmItem->infoEn
    rc = memset_s(alarmItem->infoEn, lenInfoEn, 0, lenInfoEn);
    securec_check_c(rc, "\0", "\0");
    // initialize the alarmItem->infoCh
    rc = memset_s(alarmItem->infoCh, lenInfoCh, 0, lenInfoCh);
    securec_check_c(rc, "\0", "\0");

    if (ALM_AT_Fault == type || ALM_AT_Event == type) {
        va_start(argp1, type);
        va_start(argp2, type);
        nRet = vsnprintf_s(alarmItem->infoEn, lenInfoEn, lenInfoEn - 1, AlarmIdToAlarmInfoEn(alarmItem->id), argp1);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = vsnprintf_s(alarmItem->infoCh, lenInfoCh, lenInfoCh - 1, AlarmIdToAlarmInfoCh(alarmItem->id), argp2);
        securec_check_ss_c(nRet, "\0", "\0");
        va_end(argp1);
        va_end(argp2);
    }
    FillAlarmAdditionalInfo(additionalParam, instanceName, databaseName, dbUserName, logicClusterName, alarmItem);
}

// check whether the alarm component exists
static bool CheckAlarmComponent(const char* alarmComponentPath)
{
    static int accessCount = 0;
    if (access(alarmComponentPath, F_OK) != 0) {
        if (0 == accessCount) {
            AlarmLog(ALM_LOG, "Alarm component does not exist.");
        }
        if (accessCount < 1000) {
            ++accessCount;
        } else {
            accessCount = 0;
        }
        return false;
    } else {
        return true;
    }
}

/* suppress the component alarm report, don't suppress the event report */
static bool SuppressComponentAlarmReport(Alarm* alarmItem, AlarmType type, int timeInterval)
{
    time_t thisTime = time(NULL);

    /* alarm suppression */
    if (ALM_AT_Fault == type) {                    // now the state is fault
        if (ALM_AS_Reported == alarmItem->stat) {  // original state is fault
            // check whether the interval between now and last report time is more than $timeInterval secs
            if (thisTime - alarmItem->lastReportTime >= timeInterval && alarmItem->reportCount < 5) {
                ++(alarmItem->reportCount);
                alarmItem->lastReportTime = thisTime;
                // need report
                return true;
            } else {
                // don't need report
                return false;
            }
        } else if (ALM_AS_Normal == alarmItem->stat) {  // original state is resume
            // now the state have changed, report the alarm immediately
            alarmItem->reportCount = 1;
            alarmItem->lastReportTime = thisTime;
            alarmItem->stat = ALM_AS_Reported;
            // need report
            return true;
        }
    } else if (ALM_AT_Resume == type) {            // now the state is resume
        if (ALM_AS_Reported == alarmItem->stat) {  // original state is fault
            // now the state have changed, report the resume immediately
            alarmItem->reportCount = 1;
            alarmItem->lastReportTime = thisTime;
            alarmItem->stat = ALM_AS_Normal;
            // need report
            return true;
        } else if (ALM_AS_Normal == alarmItem->stat) {  // original state is resume
            // check whether the interval between now and last report time is more than $timeInterval secs
            if (thisTime - alarmItem->lastReportTime >= timeInterval && alarmItem->reportCount < 5) {
                ++(alarmItem->reportCount);
                alarmItem->lastReportTime = thisTime;
                // need report
                return true;
            } else {
                // don't need report
                return false;
            }
        }
    } else if (ALM_AT_Event == type) {
        // report immediately
        return true;
    }

    return false;
}

// suppress the syslog alarm report, filter the resume, only report alarm
static bool SuppressSyslogAlarmReport(Alarm* alarmItem, AlarmType type, int timeInterval)
{
    time_t thisTime = time(NULL);

    if (ALM_AT_Fault == type) {
        // only report alarm and event
        if (ALM_AS_Reported == alarmItem->stat) {
            // original stat is fault
            // check whether the interval between now and the last report time is more than $timeInterval secs
            if (thisTime - alarmItem->lastReportTime >= timeInterval && alarmItem->reportCount < 5) {
                ++(alarmItem->reportCount);
                alarmItem->lastReportTime = thisTime;
                // need report
                return true;
            } else {
                // don't need report
                return false;
            }
        } else if (ALM_AS_Normal == alarmItem->stat) {
            // original state is resume
            alarmItem->reportCount = 1;
            alarmItem->lastReportTime = thisTime;
            alarmItem->stat = ALM_AS_Reported;
            return true;
        }
    } else if (ALM_AT_Event == type) {
        // report immediately
        return true;
    } else if (ALM_AT_Resume == type) {
        alarmItem->stat = ALM_AS_Normal;
        return false;
    }

    return false;
}

/* suppress the alarm log */
static bool SuppressAlarmLogReport(Alarm* alarmItem, AlarmType type, int timeInterval, int maxReportCount)
{
    struct timeval thisTime;
    gettimeofday(&thisTime, NULL);

    /* alarm suppression */
    if (type == ALM_AT_Fault) {                   /* now the state is fault */
        if (alarmItem->stat == ALM_AS_Reported) { /* original state is fault */
            /* check whether the interval between now and last report time is more than $timeInterval secs */
            if (thisTime.tv_sec - alarmItem->lastReportTime >= timeInterval &&
                alarmItem->reportCount < maxReportCount) {
                ++(alarmItem->reportCount);
                alarmItem->lastReportTime = thisTime.tv_sec;
                if (alarmItem->startTimeStamp == 0)
                    alarmItem->startTimeStamp = thisTime.tv_sec * 1000 + thisTime.tv_usec / 1000;
                /* need report */
                return false;
            } else {
                /* don't need report */
                return true;
            }
        } else if (alarmItem->stat == ALM_AS_Normal) { /* original state is resume */
            /* now the state have changed, report the alarm immediately */
            alarmItem->reportCount = 1;
            alarmItem->lastReportTime = thisTime.tv_sec;
            alarmItem->stat = ALM_AS_Reported;
            alarmItem->startTimeStamp = thisTime.tv_sec * 1000 + thisTime.tv_usec / 1000;
            alarmItem->endTimeStamp = 0;
            /* need report */
            return false;
        }
    } else if (type == ALM_AT_Resume) {           /* now the state is resume */
        if (alarmItem->stat == ALM_AS_Reported) { /* original state is fault */
            /* now the state have changed, report the resume immediately */
            alarmItem->reportCount = 1;
            alarmItem->lastReportTime = thisTime.tv_sec;
            alarmItem->stat = ALM_AS_Normal;
            alarmItem->endTimeStamp = thisTime.tv_sec * 1000 + thisTime.tv_usec / 1000;
            alarmItem->startTimeStamp = 0;
            /* need report */
            return false;
        } else if (alarmItem->stat == ALM_AS_Normal) { /* original state is resume */
            /* check whether the interval between now and last report time is more than $timeInterval secs */
            if (thisTime.tv_sec - alarmItem->lastReportTime >= timeInterval &&
                alarmItem->reportCount < maxReportCount) {
                ++(alarmItem->reportCount);
                alarmItem->lastReportTime = thisTime.tv_sec;
                if (alarmItem->endTimeStamp == 0)
                    alarmItem->endTimeStamp = thisTime.tv_sec * 1000 + thisTime.tv_usec / 1000;
                /* need report */
                return false;
            } else {
                /* don't need report */
                return true;
            }
        }
    } else if (type == ALM_AT_Event) {
        /* need report */
        alarmItem->startTimeStamp = thisTime.tv_sec * 1000 + thisTime.tv_usec / 1000;
        return false;
    }

    return true;
}

static void GetFormatLenStr(char* outputLen, int inputLen)
{
    outputLen[4] = '\0';
    outputLen[3] = '0' + inputLen % 10;
    inputLen /= 10;
    outputLen[2] = '0' + inputLen % 10;
    inputLen /= 10;
    outputLen[1] = '0' + inputLen % 10;
    inputLen /= 10;
    outputLen[0] = '0' + inputLen % 10;
}

static void ComponentReport(
    char* alarmComponentPath, Alarm* alarmItem, AlarmType type, AlarmAdditionalParam* additionalParam)
{
    int nRet = 0;
    char reportCmd[4096] = {0};
    int retCmd = 0;
    int cnt = 0;
    char tempBuff[4096] = {0};
    char clusterNameLen[5] = {0};
    char databaseNameLen[5] = {0};
    char dbUserNameLen[5] = {0};
    char hostIPLen[5] = {0};
    char hostNameLen[5] = {0};
    char instanceNameLen[5] = {0};
    char additionInfoLen[5] = {0};
    char clusterName[512] = {0};

    int i = 0;
    errno_t rc = 0;

    /* Set the host ip and the host name of the feature permission alarm to make that alarms of different hosts can be
     * suppressed. */
    if (ALM_AI_UnbalancedCluster == alarmItem->id || ALM_AI_FeaturePermissionDenied == alarmItem->id) {
        rc = memset_s(additionalParam->hostIP, sizeof(additionalParam->hostIP), 0, sizeof(additionalParam->hostIP));
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(
            additionalParam->hostName, sizeof(additionalParam->hostName), 0, sizeof(additionalParam->hostName));
        securec_check_c(rc, "\0", "\0");
    }

    if (additionalParam->logicClusterName[0] != '\0') {
        rc = snprintf_s(clusterName,
            sizeof(clusterName),
            sizeof(clusterName) - 1,
            "%s:%s",
            additionalParam->clusterName,
            additionalParam->logicClusterName);
        securec_check_ss_c(rc, "\0", "\0");
    } else {
        rc = memcpy_s(
            clusterName, sizeof(clusterName), additionalParam->clusterName, sizeof(additionalParam->clusterName));
        securec_check_ss_c(rc, "\0", "\0");
    }

    GetFormatLenStr(clusterNameLen, strlen(clusterName));
    GetFormatLenStr(databaseNameLen, strlen(additionalParam->databaseName));
    GetFormatLenStr(dbUserNameLen, strlen(additionalParam->dbUserName));
    GetFormatLenStr(hostIPLen, strlen(additionalParam->hostIP));
    GetFormatLenStr(hostNameLen, strlen(additionalParam->hostName));
    GetFormatLenStr(instanceNameLen, strlen(additionalParam->instanceName));
    GetFormatLenStr(additionInfoLen, strlen(additionalParam->additionInfo));

    for (i = 0; i < (int)strlen(additionalParam->additionInfo); ++i) {
        if (' ' == additionalParam->additionInfo[i]) {
            additionalParam->additionInfo[i] = '#';
        }
    }

    nRet = snprintf_s(tempBuff,
        sizeof(tempBuff),
        sizeof(tempBuff) - 1,
        "%s%s%s%s%s%s%s%s%s%s%s%s%s%s",
        clusterNameLen,
        databaseNameLen,
        dbUserNameLen,
        hostIPLen,
        hostNameLen,
        instanceNameLen,
        additionInfoLen,
        clusterName,
        additionalParam->databaseName,
        additionalParam->dbUserName,
        additionalParam->hostIP,
        additionalParam->hostName,
        additionalParam->instanceName,
        additionalParam->additionInfo);
    securec_check_ss_c(nRet, "\0", "\0");

    check_input_for_security1(alarmComponentPath);
    check_input_for_security1(tempBuff);
    nRet = snprintf_s(reportCmd,
        sizeof(reportCmd),
        sizeof(reportCmd) - 1,
        "%s alarm %ld %d %s",
        alarmComponentPath,
        alarmItem->id,
        type,
        tempBuff);
    securec_check_ss_c(nRet, "\0", "\0");

    do {
        retCmd = system(reportCmd);
        // return ALARM_REPORT_SUPPRESS, represent alarm report suppressed
        if (ALARM_REPORT_SUPPRESS == WEXITSTATUS(retCmd))
            break;
        if (++cnt > 3)
            break;
    } while (WEXITSTATUS(retCmd) != ALARM_REPORT_SUCCEED);

    if (ALARM_REPORT_SUCCEED != WEXITSTATUS(retCmd) && ALARM_REPORT_SUPPRESS != WEXITSTATUS(retCmd)) {
        AlarmLog(ALM_LOG, "Component alarm report failed! Cmd: %s, retCmd: %d.", reportCmd, WEXITSTATUS(retCmd));
    } else if (ALARM_REPORT_SUCCEED == WEXITSTATUS(retCmd)) {
        if (type != ALM_AT_Resume) {
            AlarmLog(ALM_LOG, "Component alarm report succeed! Cmd: %s, retCmd: %d.", reportCmd, WEXITSTATUS(retCmd));
        }
    }
}

static void SyslogReport(Alarm* alarmItem, AlarmAdditionalParam* additionalParam)
{
    int nRet = 0;
    char reportInfo[4096] = {0};

    nRet = snprintf_s(reportInfo,
        sizeof(reportInfo),
        sizeof(reportInfo) - 1,
        "%s||%s||%s||||||||%s||%s||%s||%s||%s||%s||%s||%s||%s||%s||%s||||||||||||||%s||%s||||||||||||||||||||",
        "Syslog MPPDB",
        additionalParam->hostName,
        additionalParam->hostIP,
        "Database",
        "MppDB",
        additionalParam->logicClusterName,
        "SYSLOG",
        additionalParam->instanceName,
        "Alarm",
        AlarmIdToAlarmNameEn(alarmItem->id),
        AlarmIdToAlarmNameCh(alarmItem->id),
        "1",
        "0",
        "6",
        alarmItem->infoEn,
        alarmItem->infoCh);

    securec_check_ss_c(nRet, "\0", "\0");
    syslog(LOG_ERR, "%s", reportInfo);
}

/* Check this line is comment line or not, which is in AlarmItem.conf file */
static bool isValidScopeLine(const char* str)
{
    size_t ii = 0;

    for (;;) {
        if (*(str + ii) == ' ') {
            ii++; /* skip blank */
        } else {
            break;
        }
    }

    if (*(str + ii) == '#')
        return true; /* comment line */

    return false; /* not comment line */
}

static void AlarmScopeInitialize(void)
{
    char* gaussHomeDir = NULL;
    char* subStr = NULL;
    char* subStr1 = NULL;
    char* subStr2 = NULL;
    char* saveptr1 = NULL;
    char* saveptr2 = NULL;
    char alarmItemPath[MAXPGPATH];
    char buf[MAX_BUF_SIZE] = {0};
    errno_t nRet, rc;

    if ((gaussHomeDir = gs_getenv_r("GAUSSHOME")) == NULL) {
        AlarmLog(ALM_LOG, "ERROR: environment variable $GAUSSHOME is not set!\n");
        return;
    }
    check_input_for_security1(gaussHomeDir);

    nRet = snprintf_s(alarmItemPath, MAXPGPATH, MAXPGPATH - 1, "%s/bin/alarmItem.conf", gaussHomeDir);
    securec_check_ss_c(nRet, "\0", "\0");
    canonicalize_path(alarmItemPath);
    FILE* fd = fopen(alarmItemPath, "r");
    if (fd == NULL)
        return;

    while (!feof(fd)) {
        rc = memset_s(buf, MAX_BUF_SIZE, 0, MAX_BUF_SIZE);
        securec_check_c(rc, "\0", "\0");
        if (fgets(buf, MAX_BUF_SIZE, fd) == NULL)
            continue;

        if (isValidScopeLine(buf))
            continue;

        subStr = strstr(buf, "alarm_scope");
        if (subStr == NULL)
            continue;

        subStr = strstr(subStr + strlen("alarm_scope"), "=");
        if (subStr == NULL || *(subStr + 1) == '\0') /* '=' is last char */
            continue;

        int ii = 1;
        for (;;) {
            if (*(subStr + ii) == ' ') {
                ii++; /* skip blank */
            } else
                break;
        }

        subStr = subStr + ii;
        subStr1 = strtok_r(subStr, "\n", &saveptr1);
        if (subStr1 == NULL)
            continue;
        subStr2 = strtok_r(subStr1, "\r", &saveptr2);
        if (subStr2 == NULL)
            continue;
        rc = memcpy_s(g_alarm_scope, MAX_BUF_SIZE, subStr2, strlen(subStr2));
        securec_check_c(rc, "\0", "\0");
    }
    fclose(fd);
}

void AlarmReporter(Alarm* alarmItem, AlarmType type, AlarmAdditionalParam* additionalParam)
{
    if (NULL == alarmItem) {
        AlarmLog(ALM_LOG, "alarmItem is NULL.");
        return;
    }
    if (0 == strcmp(WarningType, FUSIONINSIGHTTYPE)) {  // the warning type is FusionInsight type
        // check whether the alarm component exists
        if (false == CheckAlarmComponent(g_alarmComponentPath)) {
            // the alarm component does not exist
            return;
        }
        // suppress the component alarm
        if (true ==
            SuppressComponentAlarmReport(alarmItem, type, g_alarmReportInterval)) {  // check whether report the alarm
            ComponentReport(g_alarmComponentPath, alarmItem, type, additionalParam);
        }
    } else if (0 == strcmp(WarningType, ICBCTYPE)) {  // the warning type is ICBC type
        // suppress the syslog alarm
        if (true ==
            SuppressSyslogAlarmReport(alarmItem, type, g_alarmReportInterval)) {  // check whether report the alarm
            SyslogReport(alarmItem, additionalParam);
        }
    } else if (strcmp(WarningType, CBGTYPE) == 0) {
        if (!SuppressAlarmLogReport(alarmItem, type, g_alarmReportInterval, g_alarmReportMaxCount))
            write_alarm(alarmItem,
                AlarmIdToAlarmNameEn(alarmItem->id),
                AlarmIdToAlarmLevel(alarmItem->id),
                type,
                additionalParam);
    }
}

/*
---------------------------------------------------------------------------
The first report method:

We register check function in the alarm module.
And we will check and report all the alarm(alarm or resume) item in a loop.
---------------------------------------------------------------------------

---------------------------------------------------------------------------
The second report method:

We don't register any check function in the alarm module.
And we don't initialize the alarm item(typedef struct Alarm) structure here.
We will initialize the alarm item(typedef struct Alarm) in the begining of alarm module.
We invoke report function internally in the monitor process.
We fill the report message and then invoke the AlarmReporter.
---------------------------------------------------------------------------

---------------------------------------------------------------------------
The third report method:

We don't register any check function in the alarm module.
When we detect some errors occur, we will report some alarm.
Firstly, initialize the alarm item(typedef struct Alarm).
Secondly, fill the report message(typedef struct AlarmAdditionalParam).
Thirdly, invoke the AlarmReporter, report the alarm.
---------------------------------------------------------------------------
*/
void AlarmCheckerLoop(Alarm* checkList, int checkListSize)
{
    int i;
    AlarmAdditionalParam tempAdditionalParam;

    if (NULL == checkList || checkListSize <= 0) {
        AlarmLog(ALM_LOG, "AlarmCheckerLoop failed.");
        return;
    }

    for (i = 0; i < checkListSize; ++i) {
        Alarm* alarmItem = &(checkList[i]);
        AlarmCheckResult result = ALM_ACR_UnKnown;

        AlarmType type = ALM_AT_Fault;

        if (alarmItem->checker != NULL) {
            // execute alarm check function and output check result
            result = alarmItem->checker(alarmItem, &tempAdditionalParam);
            if (ALM_ACR_UnKnown == result) {
                continue;
            }
            if (ALM_ACR_Normal == result) {
                type = ALM_AT_Resume;
            }
            (void)AlarmReporter(alarmItem, type, &tempAdditionalParam);
        }
    }
}

void AlarmLog(int level, const char* fmt, ...)
{
    va_list args;
    char buf[MAXPGPATH] = {0}; /*enough for log module*/
    int nRet = 0;

    (void)va_start(args, fmt);
    nRet = vsnprintf_s(buf, sizeof(buf), sizeof(buf) - 1, fmt, args);
    securec_check_ss_c(nRet, "\0", "\0");
    va_end(args);

    AlarmLogImplementation(level, AlarmLogPrefix, buf);
}

/*
Initialize the alarm item
reportTime:  express the last time of alarm report. the default value is 0.
*/
void AlarmItemInitialize(
    Alarm* alarmItem, AlarmId alarmId, AlarmStat alarmStat, CheckerFunc checkerFunc, time_t reportTime, int reportCount)
{
    alarmItem->checker = checkerFunc;
    alarmItem->id = alarmId;
    alarmItem->stat = alarmStat;
    alarmItem->lastReportTime = reportTime;
    alarmItem->reportCount = reportCount;
    alarmItem->startTimeStamp = 0;
    alarmItem->endTimeStamp = 0;
}
