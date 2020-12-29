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
 * callback.cpp
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/bin/license_checker/callback.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "callback.h"
#include <string.h>
#include <errno.h>
#ifdef __cplusplus
extern "C" {
#endif

/************************************************************************/
/* LIC_CALLBACK_TYPE_DYNAMIC_MALLOC */
/************************************************************************/
LIC_VOID* OS_DynMalloc(LIC_ULONG ulSize)
{
    return (LIC_VOID*)malloc(ulSize);
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_DYNAMIC_FREE */
/************************************************************************/
LIC_VOID OS_DynFree(LIC_VOID* pAddr)
{
    free(pAddr);
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_MUTEX_CREATE */
/************************************************************************/
LIC_ULONG OS_SmMCreate(LIC_CHAR* pszName, LIC_ULONG* pSm)
{
    pSm = (LIC_ULONG*)sem_open(pszName, O_CREAT | O_RDWR, 0600, 1);
    if (pSm == (LIC_ULONG*)SEM_FAILED) {
        return LIC_ERROR;
    }
    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_MUTEX_DELETE */
/************************************************************************/
LIC_ULONG OS_SmDelete(LIC_ULONG ulSmID)
{
    if (sem_close((sem_t*)&ulSmID) == -1)
        return LIC_ERROR;
    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_MUTEX_ACQUIRE */
/************************************************************************/
LIC_ULONG OS_SmP(LIC_ULONG ulSem, LIC_ULONG ulTimeOutInMillSec)
{
    if (sem_wait((sem_t*)&ulSem) == -1)
        return LIC_ERROR;
    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_MUTEX_RELEASE */
/************************************************************************/
LIC_ULONG OS_SmV(LIC_ULONG ulSmID)
{
    if (sem_post((sem_t*)&ulSmID) == -1)
        return LIC_ERROR;
    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_GET_TIME */
/************************************************************************/
LIC_ULONG OS_GetSysTime(LIC_SYS_T_STRU* pSysTime)
{

    time_t t;
    struct tm* area;
    t = time(NULL);
    area = localtime(&t);
    if (area == NULL)
        return LIC_ERROR;
    pSysTime->uwYear = 1900 + area->tm_year;
    pSysTime->ucMonth = area->tm_mon + 1;
    pSysTime->ucDate = area->tm_mday;
    pSysTime->ucHour = area->tm_hour;
    pSysTime->ucMinute = area->tm_min;
    pSysTime->ucSecond = area->tm_sec;
    pSysTime->ucWeek = area->tm_wday;
    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_START_TIMER */
/************************************************************************/
LIC_ULONG OS_StartTimer(LIC_ULONG* pTmHandler, LIC_ULONG ulLength, LIC_SYS_T_STRU* pTime,
    LIC_TIMER_CALLBACK_FUNC pTmCallBack, LIC_ULONG ulMode, LIC_ULONG ulType, LIC_ULONG ulTimerId)
{
    if (ulType != LIC_TIMER_TYPE_REL)
        return LIC_ERROR;

    struct itimerval tick = {0};
    signal(SIGALRM, (void (*)(int))pTmCallBack);
    errno_t ss_rc = 0;
    ss_rc = memset_s(&tick, sizeof(tick), 0x0, sizeof(tick));
    securec_check_c(ss_rc, "\0", "\0");

    if (ulMode == LIC_TIMER_MODE_ONCE) {
        tick.it_value.tv_sec = ulLength / 1000;
        tick.it_value.tv_usec = (ulLength % 1000) * 1000;
    }
    if (ulMode == LIC_TIMER_MODE_REPEAT) {
        tick.it_value.tv_sec = ulLength / 1000;
        tick.it_value.tv_usec = (ulLength % 1000) * 1000;
        tick.it_interval.tv_sec = ulLength / 1000;
        tick.it_interval.tv_usec = (ulLength % 1000) * 1000;
    }
    if (setitimer(ITIMER_REAL, &tick, NULL) < 0)
        return LIC_ERROR;

    *pTmHandler = (LIC_ULONG)random();

    return LIC_OK;
}

/************************************************************************/
/* LIC_CALLBACK_TYPE_STOP_TIMER */
/************************************************************************/
LIC_ULONG OS_StopTimer(LIC_ULONG ulTmHandler)
{
    struct itimerval tick = {0};
    errno_t ss_rc = 0;
    ss_rc = memset_s(&tick, sizeof(tick), 0x0, sizeof(tick));
    securec_check_c(ss_rc, "\0", "\0");
    if (setitimer(ITIMER_REAL, &tick, NULL) < 0)
        return LIC_ERROR;

    return LIC_OK;
}

/***********************************************************************************************/
/*
ALM_CALLBACK_TYPE_GET_MACHINE_ESN
ALM_CALLBACK_TYPE_IO_READ_FUNC
ALM_CALLBACK_TYPE_IO_WRITE_FUNC
*/
/***********************************************************************************************/

/************************************************************************/
/* ALM_CALLBACK_TYPE_GET_MACHINE_ESN    			*/
/************************************************************************/
LIC_ULONG APP_GetMachineEsn(LIC_ULONG* pulCount, LIC_MACHINEID_STRU* pstMachineId)
{
    errno_t ss_rc = 0;

    *pulCount = 1;
    ss_rc = strncpy_s(
        (char*)(pstMachineId->custom_machineprint), LIC_LONG_STREAM_LEN, "GAUSSMPPDB", LIC_LONG_STREAM_LEN - 1);
    securec_check_c(ss_rc, "\0", "\0");
    pstMachineId->custom_machineprint[LIC_LONG_STREAM_LEN - 1] = '\0';
    return LIC_OK;
}

/************************************************************************/
/************************************************************************/
LIC_ULONG APP_IOReadCallback(LIC_ULONG ulType, LIC_VOID* pBuf, LIC_ULONG* pulLen, LIC_ULONG ulOffSet)
{
    return LIC_OK;
}

/************************************************************************/
/* ALM_CALLBACK_TYPE_IO_WRITE_FUNC	           */
/************************************************************************/
LIC_ULONG APP_IOWriteCallback(LIC_ULONG ulType, LIC_VOID* pBuf, LIC_ULONG ulLen, LIC_ULONG ulOffSet)
{
    return LIC_OK;
}

#ifdef __cplusplus
}
#endif /* __cpluscplus */
