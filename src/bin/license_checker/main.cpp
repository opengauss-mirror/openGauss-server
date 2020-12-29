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
 * main.cpp
 *     main functions of license_checker
 * 
 * IDENTIFICATION
 *        src/bin/license_checker/main.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "callback.h"
#include <sys/stat.h>
#define RWRWRW (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
#define FILE_NOT_EXIST "2000000000"

#ifdef __cplusplus
extern "C" {
#endif

/************************************************************************/
/* Register system callback functions */
/************************************************************************/
LIC_ULONG registerOSCallback()
{
    LIC_ULONG ulRet = LIC_OK;

    LIC_CALLBACK_FUNCTION_STRU stOSCallbackFunc[LIC_OS_CALLBACK_NUM] = {
        {LIC_CALLBACK_TYPE_DYNAMIC_MALLOC, (LIC_VOID*)OS_DynMalloc},
        {LIC_CALLBACK_TYPE_DYNAMIC_FREE, (LIC_VOID*)OS_DynFree},

        {LIC_CALLBACK_TYPE_MUTEX_CREATE, (LIC_VOID*)OS_SmMCreate},
        {LIC_CALLBACK_TYPE_MUTEX_DELETE, (LIC_VOID*)OS_SmDelete},
        {LIC_CALLBACK_TYPE_MUTEX_ACQUIRE, (LIC_VOID*)OS_SmP},
        {LIC_CALLBACK_TYPE_MUTEX_RELEASE, (LIC_VOID*)OS_SmV},

        {LIC_CALLBACK_TYPE_GET_TIME, (LIC_VOID*)OS_GetSysTime},
        {LIC_CALLBACK_TYPE_START_TIMER, (LIC_VOID*)OS_StartTimer},
        {LIC_CALLBACK_TYPE_STOP_TIMER, (LIC_VOID*)OS_StopTimer}};

    ulRet = ALM_RegisterCallback(LIC_OS_CALLBACK_NUM, stOSCallbackFunc);
    if (ulRet != LIC_OK) {
        printf("Register OS Callback failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    return ulRet;
}

/************************************************************************/
/* Register app callback functions */
/************************************************************************/
LIC_ULONG registerAppCallback()
{
    LIC_ULONG ulRet = LIC_OK;

    LIC_CALLBACK_FUNCTION_STRU stAppCallbackFunc[LIC_APP_CALLBACK_NUM] = {
        {ALM_CALLBACK_TYPE_IO_READ_FUNC, (LIC_VOID*)APP_IOReadCallback},
        {ALM_CALLBACK_TYPE_IO_WRITE_FUNC, (LIC_VOID*)APP_IOWriteCallback},
        {ALM_CALLBACK_TYPE_GET_MACHINE_ESN, (LIC_VOID*)APP_GetMachineEsn}};

    ulRet = ALM_RegisterCallback(LIC_APP_CALLBACK_NUM, stAppCallbackFunc);
    if (ulRet != LIC_OK) {
        printf("Register App Callback failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    return ulRet;
}

/************************************************************************/
/* Init license manager */
/************************************************************************/
LIC_ULONG initLicense()
{
    LIC_ULONG ulRet = LIC_OK;

    LIC_MANAGER_STATIC_CONFIG_STRU stConf = {0};

    stConf.ulManagerMode = LIC_STANDALONE;
    stConf.ulSafeMemSize = 0x400;
    stConf.ulLKMemSize = 0x40000;
    stConf.ulSecSafeMemSize = 0x400;

    ulRet = ALM_Init(&stConf);
    if (ulRet != LIC_OK) {
        printf("Init License failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    return LIC_OK;
}
/*****************************************************************************/
/*****************************************************************************/

LIC_ULONG registerBbomControlInfo()
{

    LIC_ULONG ulRet = LIC_OK;
    const int ulResCount = LIC_RES_MAX_NUM;

    int i = 0;
    LIC_CHAR acBuf[LIC_ITEM_NAME_MAX_LEN + 1] = {0};
    LIC_CHAR acDes[LIC_ITEM_DESC_MAX_LEN + 1] = {0};
    LIC_STATIC_ITEM_INFO_STRU* pstResInfo = LIC_NULL_PTR;

    pstResInfo = (LIC_STATIC_ITEM_INFO_STRU*)malloc(ulResCount * sizeof(LIC_STATIC_ITEM_INFO_STRU));
    if (LIC_NULL_PTR == pstResInfo) {
        printf("\r\n The memory alloc fail for stResInfo");
        return LIC_ERR_MEM_ALLOC_FAIL;
    }
    errno_t ss_rc = 0;
    ss_rc = memset_s(pstResInfo,
        sizeof(LIC_STATIC_ITEM_INFO_STRU) * ulResCount,
        0x0,
        sizeof(LIC_STATIC_ITEM_INFO_STRU) * ulResCount);
    securec_check_c(ss_rc, "\0", "\0");

    for (i = 0; i < ulResCount; i++) {
        pstResInfo[i].ulItemId = i + 1;
        pstResInfo[i].ulItemType = LIC_RESOURCE_CONTROL;
        pstResInfo[i].ulDefaultVal = 9;
        pstResInfo[i].ulResMinmVal = 1;
        pstResInfo[i].ulResMaxVal = MAX_INT_NUM;

        int ret;
        ret = snprintf_s(acBuf, sizeof(acBuf), sizeof(acBuf), "LFH00MPPDB%02d", i + 1);
        securec_check_ss_c(ret, "\0", "\0");
        ss_rc = strncpy_s(pstResInfo[i].acItemName, LIC_ITEM_NAME_MAX_LEN, acBuf, LIC_ITEM_NAME_MAX_LEN - 1);
        securec_check_c(ss_rc, "\0", "\0");
        pstResInfo[i].acItemName[LIC_ITEM_NAME_MAX_LEN] = '\0';

        ret = snprintf_s(acDes, sizeof(acDes), sizeof(acDes), "Des%02d", i + 1);
        securec_check_ss_c(ret, "\0", "\0");
        ss_rc = strncpy_s(pstResInfo[i].acItemDesc, LIC_ITEM_DESC_MAX_LEN, acDes, LIC_ITEM_DESC_MAX_LEN - 1);
        securec_check_c(ss_rc, "\0", "\0");
        pstResInfo[i].acItemDesc[LIC_ITEM_DESC_MAX_LEN] = '\0';

        pstResInfo[i].ulResUnitType = LIC_RES_COUNT_STATIC;
    }

    ulRet = ALM_RegisterBbomControlInfo(ulResCount, pstResInfo);
    if (LIC_OK != ulRet) {
        printf("Register Resource failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        free(pstResInfo);
        return ulRet;
    }

    free(pstResInfo);
    return LIC_OK;
}
/*****************************************************************************/
/*****************************************************************************/
LIC_ULONG readFileToBuf(LIC_CHAR* filePath, LIC_CHAR* pBuf, LIC_ULONG* pulFileLen)
{
    FILE* pFile = LIC_NULL_PTR;
    LIC_ULONG ulLength = 0;
    LIC_ULONG ulRet = LIC_OK;

    if (LIC_NULL_PTR == filePath) {
        printf("File Path is NULL\n");
        return LIC_ERROR;
    }

    pFile = fopen(filePath, "rb+");
    if (LIC_NULL_PTR == pFile) {
        printf("[%s] File %s Open Failed\n", FILE_NOT_EXIST, filePath);
        return LIC_ERROR;
    }

    fseek(pFile, 0, 2);

    ulLength = ftell(pFile);
    if (ulLength < 0) {
        fclose(pFile);
        return LIC_ERROR;
    }

    if (ulLength < *pulFileLen) {
        *pulFileLen = ulLength;
    } else {
        fclose(pFile);
        return LIC_ERROR;
    }

    ulRet = fseek(pFile, 0, 0);
    if (ulRet != 0) {
        fclose(pFile);
        return LIC_ERROR;
    }

    /* Now read the contents of file into the buffer. On success it returns
    the number of bytes read, which can be checked. */
    ulRet = fread(pBuf, 1, *pulFileLen, pFile);
    if (ulRet != *pulFileLen) {
        fclose(pFile);
        return LIC_ERROR;
    }

    ulRet = fclose(pFile);
    if (ulRet != 0) {
        return LIC_ERROR;
    }

    return LIC_OK;
}

/*****************************************************************************/
/*****************************************************************************/
LIC_ULONG setProductBasicData()
{
    LIC_BASIC_DATA_STRU stBasicData = {0};
    LIC_ULONG ulRet = LIC_OK;
    errno_t ss_rc = 0;
    ss_rc = strncpy_s(stBasicData.acProduct, LIC_PRDNAME_MAX_LEN, "FusionInsight", LIC_PRDNAME_MAX_LEN - 1);
    securec_check_c(ss_rc, "\0", "\0");
    stBasicData.acProduct[LIC_PRDNAME_MAX_LEN] = '\0';
    ss_rc = strncpy_s(stBasicData.acVersion, LIC_PRDVER_MAX_LEN, "V100R002", LIC_PRDVER_MAX_LEN - 1);
    securec_check_c(ss_rc, "\0", "\0");
    stBasicData.acVersion[LIC_PRDVER_MAX_LEN] = '\0';
    stBasicData.pacProdSecData = key_dat2;

    ulRet = ALM_SetProductBasicData(&stBasicData);
    if (LIC_OK != ulRet) {
        printf("Set Product Basic Data Failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }
    return LIC_OK;
}
/*****************************************************************************/
/*****************************************************************************/
LIC_ULONG enable()
{
    LIC_ULONG ulRet = LIC_OK;
    ulRet = ALM_Enable();

    if (LIC_OK != ulRet) {
        printf("Enable failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
/************************************************************************/
LIC_ULONG verifyLicense(char* str)
{
    LIC_ULONG ulRet = LIC_OK;
    LIC_CHAR* pBuf = LIC_NULL_PTR;
    LIC_ULONG uiFileLen = 1400000;
    LIC_VERIFY_RESULT_STRU* pstVerifyResult = LIC_NULL_PTR;

    pBuf = (LIC_CHAR*)malloc(uiFileLen);
    if (NULL == pBuf) {
        return LIC_ERROR;
    }
    errno_t ss_rc = 0;
    ss_rc = memset_s(pBuf, uiFileLen, 0x0, uiFileLen);
    securec_check_c(ss_rc, "\0", "\0");
    ulRet = readFileToBuf(str, pBuf, &uiFileLen);
    if (LIC_OK != ulRet) {
        free(pBuf);
        return LIC_ERROR;
    }

    // Max item count
    pstVerifyResult = (LIC_VERIFY_RESULT_STRU*)malloc(sizeof(LIC_VERIFY_RESULT_STRU));

    if (LIC_NULL_PTR == pstVerifyResult) {
        free(pBuf);
        return LIC_ERROR;
    }
    ss_rc = memset_s(pstVerifyResult, sizeof(LIC_VERIFY_RESULT_STRU), 0x0, sizeof(LIC_VERIFY_RESULT_STRU));
    securec_check_c(ss_rc, "\0", "\0");

    pstVerifyResult->ulItemCount = LIC_RES_MAX_NUM;
    pstVerifyResult->pstItemResult =
        (LIC_ITEM_RESULT_STRU*)malloc(pstVerifyResult->ulItemCount * sizeof(LIC_ITEM_RESULT_STRU));
    if (LIC_NULL_PTR == pstVerifyResult->pstItemResult) {
        free(pstVerifyResult);
        free(pBuf);
        return LIC_ERROR;
    }
    ss_rc = memset_s(pstVerifyResult->pstItemResult, pstVerifyResult->ulItemCount, 0x0, pstVerifyResult->ulItemCount);
    securec_check_c(ss_rc, "\0", "\0");

    if (LIC_OK != (ulRet = ALM_VerifyLicenseKey(uiFileLen, pBuf, pstVerifyResult))) {
        printf("Verify License failed, %lu, %s\n", ulRet, ALM_GetErrMessage(ulRet));
        free(pstVerifyResult->pstItemResult);
        free(pstVerifyResult);
        free(pBuf);
        return LIC_ERROR;
    }

    free(pstVerifyResult->pstItemResult);
    free(pstVerifyResult);
    free(pBuf);
    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
LIC_ULONG activeLicense(char* filename)
{
    LIC_ULONG ulRet = LIC_OK;
    LIC_CHAR* pBuf = LIC_NULL_PTR;
    LIC_ULONG uiFileLen = 0x4000;
    LIC_VERIFY_RESULT_STRU* pstVerifyResult = LIC_NULL_PTR;

    pBuf = (LIC_CHAR*)malloc(uiFileLen);
    if (NULL == pBuf) {
        return LIC_ERROR;
    }
    errno_t ss_rc = 0;
    ss_rc = memset_s(pBuf, uiFileLen, 0x0, uiFileLen);
    securec_check_c(ss_rc, "\0", "\0");
    ulRet = readFileToBuf(filename, pBuf, &uiFileLen);
    if (LIC_OK != ulRet) {
        free(pBuf);
        return LIC_ERROR;
    }
    // Max item count
    pstVerifyResult = (LIC_VERIFY_RESULT_STRU*)malloc(sizeof(LIC_VERIFY_RESULT_STRU));
    if (LIC_NULL_PTR == pstVerifyResult) {
        free(pBuf);
        return LIC_ERROR;
    }
    ss_rc = memset_s(pstVerifyResult, sizeof(LIC_VERIFY_RESULT_STRU), 0x0, sizeof(LIC_VERIFY_RESULT_STRU));
    securec_check_c(ss_rc, "\0", "\0");

    pstVerifyResult->ulItemCount = LIC_RES_MAX_NUM;
    pstVerifyResult->pstItemResult =
        (LIC_ITEM_RESULT_STRU*)malloc(pstVerifyResult->ulItemCount * sizeof(LIC_ITEM_RESULT_STRU));
    if (LIC_NULL_PTR == pstVerifyResult->pstItemResult) {
        free(pstVerifyResult);
        free(pBuf);
        return LIC_ERROR;
    }
    ss_rc = memset_s(pstVerifyResult->pstItemResult, pstVerifyResult->ulItemCount, 0x0, pstVerifyResult->ulItemCount);
    securec_check_c(ss_rc, "\0", "\0");

    ulRet = ALM_ActivateLicenseKey(uiFileLen, pBuf, LIC_TRUE, pstVerifyResult);
    if (LIC_OK != ulRet) {
        printf("Active License failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        free(pstVerifyResult->pstItemResult);
        free(pstVerifyResult);
        free(pBuf);
        return LIC_ERROR;
    }

    free(pstVerifyResult->pstItemResult);
    free(pstVerifyResult);
    free(pBuf);
    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
LIC_ULONG getLicenseInfo()
{
    LIC_ULONG ulRet = LIC_OK;
    LIC_ULONG ulItemCnt = 0;
    LIC_COUNT_INFO_STRU stLicCnt = {0};
    LIC_LICENSE_KEYINFO_STRU stLicKeyInfo = {0};
    LIC_LICENSE_KEYITEMINFO_STRU* pstItemInfoList = LIC_NULL_PTR;

    ulRet = ALM_GetLicenseCountInfo(&stLicCnt);
    if (ulRet != LIC_OK) {
        printf("Get License Count Failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    ulItemCnt = stLicCnt.ulLkItemCnt;
    pstItemInfoList = (LIC_LICENSE_KEYITEMINFO_STRU*)malloc(ulItemCnt * sizeof(LIC_LICENSE_KEYITEMINFO_STRU));

    if (LIC_NULL_PTR == pstItemInfoList) {
        printf("Memory Malloc failed\n");
        return LIC_ERR_MEM_ALLOC_FAIL;
    }
    errno_t ss_rc = 0;
    ss_rc = memset_s(pstItemInfoList,
        ulItemCnt * sizeof(LIC_LICENSE_KEYITEMINFO_STRU),
        0x0,
        ulItemCnt * sizeof(LIC_LICENSE_KEYITEMINFO_STRU));
    securec_check_c(ss_rc, "\0", "\0");

    ulRet = ALM_GetLicenseKeyInfo(&stLicKeyInfo, ulItemCnt, pstItemInfoList);
    if (ulRet != LIC_OK) {
        printf("Get License Item Info Failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        free(pstItemInfoList);
        return ulRet;
    }
    printf("License file general information:\n");
    printf("File CopyRight : %s", stLicKeyInfo.stLkGeneralInfo.acFileCopyRight);
    printf("File Create Time : %s\n", stLicKeyInfo.stLkGeneralInfo.acFileCreateTime);
    printf("File Creator : %s\n", stLicKeyInfo.stLkGeneralInfo.acFileCreator);
    printf("File Format Version : %s\n", stLicKeyInfo.stLkGeneralInfo.acFileFormatVersion);
    printf("Product Version : %s\n", stLicKeyInfo.stLkGeneralInfo.acFilePrdVersion);
    printf("Product Name : %s\n", stLicKeyInfo.stLkGeneralInfo.acFileProduct);
    printf("File Serial Number : %s\n", stLicKeyInfo.stLkGeneralInfo.acFileSN);
    printf("Grace Day : %lu\n", stLicKeyInfo.stLkGeneralInfo.ulLkGraceDay);
    printf("License Type : %lu\n", stLicKeyInfo.stLkGeneralInfo.ulLkType);

    printf("License file customer information:\n");
    printf("Country : %s\n", stLicKeyInfo.stLkCustomerInfo.acFileCountry);
    printf("Custom : %s\n", stLicKeyInfo.stLkCustomerInfo.acFileCustom);
    printf("Office : %s\n", stLicKeyInfo.stLkCustomerInfo.acFileOffice);

    printf("Esn : %s\n", stLicKeyInfo.stLkNodeInfo.acFileEsn);
    printf("License Deadline Time: %s\n", pstItemInfoList[0].acSwDeadline);
    printf("Data Node Num: 4096\n");
    free(pstItemInfoList);
    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
LIC_ULONG getLicenseTypeInfo()
{
    LIC_ULONG ulRet = LIC_OK;
    LIC_LICENSE_KEY_TYPE_INFO_STRU stLicKeyTypeInfo = {0};
    const LIC_CHAR* acLkType[] = {
        "LIC_LICENSE_KEY_TYPE_COMM", "LIC_LICENSE_KEY_TYPE_DEMO", "LIC_LICENSE_KEY_TYPE_BUTT"};
    const LIC_CHAR* acDeadlineType[] = {"LIC_LICENSE_KEY_DEADLINE_PERMANENT",
        "LIC_LICENSE_KEY_DEADLINE_FIXED",
        "LIC_LICENSE_KEY_DEADLINE_MIXED",
        "LIC_LICENSE_KEY_DEADLINE_BUTT"};

    ulRet = ALM_GetLicenseKeyTypeInfo(&stLicKeyTypeInfo);
    if (LIC_OK != ulRet) {
        printf("ALM_GetLicenseKeyTypeInfo failed, %lu, %s", ulRet, ALM_GetErrMessage(ulRet));
        return ulRet;
    }

    printf("License File Type: %s.\n", acLkType[stLicKeyTypeInfo.ulLkType]);  // COMM:0  DEMO:1
    printf(
        "License DeadLine Type: %s.\n", acDeadlineType[stLicKeyTypeInfo.ulLkDeadline]);  // Perm:0  Temp:1 Perm+Temp:2

    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
/* Manager initialization */
LIC_ULONG ManagerInitApplication()
{
    LIC_ULONG ulRet = LIC_OK;
    // Set log level
    ulRet = ALM_SetLogLevel(LIC_ERROR_LOG);
    if (ulRet != LIC_OK) {
        printf("Set log level failed!\n");
        return ulRet;
    }
    // Register OS callbacks
    ulRet = registerOSCallback();
    if (ulRet != LIC_OK) {
        printf("ALM_RegisterCallback failed for OS callbacks\n");
        return ulRet;
    }

    // Register Application callbacks
    ulRet = registerAppCallback();
    if (ulRet != LIC_OK) {
        printf("ALM_RegisterCallback failed for APP callbacks\n");
        return ulRet;
    }

    // Init the manager
    ulRet = initLicense();
    if (ulRet != LIC_OK) {
        printf("ALM_Init failed\n");
        return ulRet;
    }

    // Register item static information
    ulRet = registerBbomControlInfo();
    if (ulRet != LIC_OK) {
        printf("ALM_RegisterStaticControlInfo failed\n");
        return ulRet;
    }

    // Set product key
    ulRet = setProductBasicData();
    if (ulRet != LIC_OK) {
        printf("ALM_SetProductKey failed\n");
        return ulRet;
    }

    // Enable the manager
    ulRet = enable();
    if (ulRet != LIC_OK) {
        printf("ALM_Enable failed\n");
        return ulRet;
    }

    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
LIC_ULONG ManagerBasicLicenseApplication(char* str)
{
    LIC_ULONG ulRet = LIC_OK;
    ulRet = verifyLicense(str);
    if (ulRet != LIC_OK) {
        return ulRet;
    }
    // Activate the normal license file
    ulRet = activeLicense(str);
    if (ulRet != LIC_OK) {
        printf("ALM_ActivateLicenseKey failed\n");
        return ulRet;
    }
    // Query License Key Type, Deadline Type, Policy Type
    ulRet = getLicenseTypeInfo();
    if (ulRet != LIC_OK) {
        printf("ALM_GetLicenseKeyTypeInfo failed\n");
        return ulRet;
    }
    // Query the item control value and item state. Current control value will be
    // same as LK value and item state will be Normal.
    ulRet = getLicenseInfo();
    if (ulRet != LIC_OK) {
        printf("get license info failed\n");
        return ulRet;
    }

    return LIC_OK;
}
int main(int argc, char** argv)
{

    if (argc < 2) {
        printf("------------------------------\n");
        printf("Usage:\n");
        printf("%s License-file\n", argv[0]);
        printf("------------------------------\n");
        return LIC_ERROR;
    }
    umask(S_IXUSR | S_IXGRP | S_IXOTH);
    if (LIC_OK != ManagerInitApplication()) {
        return LIC_ERROR;
    }

    if (LIC_OK != ManagerBasicLicenseApplication(argv[1])) {
        return LIC_ERROR;
    }
    printf("[1000000000] License checked Success!\n");
    return LIC_OK;
}

#ifdef __cplusplus
}
#endif /* __cpluscplus */
