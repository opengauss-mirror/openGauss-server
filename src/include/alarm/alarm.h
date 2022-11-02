/* ---------------------------------------------------------------------------------------
 * 
 * alarm.h
 *        openGauss alarm reporting/logging definitions.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/alarm.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ALARM_H
#define ALARM_H

#include "c.h"

/*GaussDB alarm module.*/
typedef enum AlarmId {
    ALM_AI_Unknown = 0,

    /*alarm on data instances(alarm checker)*/
    ALM_AI_MissingDataInstDataOrRedoDir = 0x404E0001,
    ALM_AI_MissingDataInstWalSegmt = 0x404E0003,
    ALM_AI_TooManyDataInstConn = 0x404F0001,

    /*alarm on monitor instances*/
    ALM_AI_AbnormalGTMInst = 0x404F0002,
    ALM_AI_AbnormalDatanodeInst = 0x404F0004,

    ALM_AI_AbnormalGTMProcess = 0x404F0008,
    ALM_AI_AbnormalCoordinatorProcess = 0x404F0009,
    ALM_AI_AbnormalDatanodeProcess = 0x404F0010,

    ALM_AI_DatanodeSwitchOver = 0x404F0021,
    ALM_AI_DatanodeFailOver = 0x404F0022,

    ALM_AI_GTMSwitchOver = 0x404F0023,
    ALM_AI_GTMFailOver = 0x404F0024,

    ALM_AI_ForceFinishRedo = 0x404F0025,

    ALM_AI_AbnormalCMSProcess = 0x404F003B,
    ALM_AI_UnbalancedCluster = 0x404F0038,
    ALM_AI_AbnormalCMAProcess = 0x404F003A,
    ALM_AI_AbnormalETCDProcess = 0x404F003E,

    /*alarm when occur errors*/
    ALM_AI_AbnormalDataHAInstListeningSocket = 0x404F0039,
    ALM_AI_AbnormalGTMSocket = 0x404F003D,
    ALM_AI_AbnormalDataInstConnToGTM = 0x404F003C,
    ALM_AI_AbnormalDataInstConnAuthMethod = 0x404F0036,
    ALM_AI_TooManyDatabaseConn = 0x404F0037,
    ALM_AI_TooManyDbUserConn = 0x404F0033,
    ALM_AI_DataInstLockFileExist = 0x404F0034,
    ALM_AI_InsufficientDataInstFileDesc = 0x404F0031,
    ALM_AI_AbnormalDataInstArch = 0x404F0032,
    ALM_AI_AbnormalTableSkewness = 0x404F0035,

    ALM_AI_AbnormalTDEFile = 0x404F0041,
    ALM_AI_AbnormalTDEValue = 0x404F0042,
    ALM_AI_AbnormalConnToKMS = 0x404F0043,

    ALM_AI_AbnormalDataInstDisk = 0x404F0047,

    ALM_AI_AbnormalEtcdDown = 0x404F0048,
    ALM_AI_AbnormalEtcdUnhealth = 0x404F0049,

    ALM_AI_AbnormalPhonyDead = 0x404F004A,
    ALM_AI_AbnormalCmaConnFail = 0x404F004B,
    ALM_AI_Build = 0x404F004C,
    ALM_AI_AbnormalBuild = 0x404F004D,
    ALM_AI_TransactionReadOnly = 0x404F0059,
    ALM_AI_ServerSwitchOver = 0x404F005A,

    ALM_AI_AbnormalEtcdNearQuota = 0x404F005B,

    ALM_AI_StorageThresholdPreAlarm = 0x404F005C,
    ALM_AI_StorageDilatationAlarmNotice = 0x404F005D,
    ALM_AI_StorageDilatationAlarmMajor = 0x404F005E,

    ALM_AI_FeaturePermissionDenied = 0x404F0050, /* No permission to invoke specific features. */
    ALM_AI_DNReduceSyncList = 0x404F005F,
    ALM_AI_DNIncreaseSyncList = 0x404F0060,
    ALM_AI_PgxcNodeMismatch = 0x404F0061,
    ALM_AI_StreamingDisasterRecoveryCnDisconnected = 0x404F0070,
    ALM_AI_StreamingDisasterRecoveryDnDisconnected = 0x404F0071,
    ALM_AI_BUTT = 0x7FFFFFFFFFFFFFFF             /*force compiler to decide AlarmId as uint64*/
} AlarmId;

typedef struct AlarmName {
    AlarmId id;
    char nameEn[256];
    char nameCh[256];
    char alarmInfoEn[1024];
    char alarmInfoCh[1024];
    char alarmLevel[256];
} AlarmName;

typedef struct AlarmAdditionalParam {
    char clusterName[256];
    char hostName[256];
    char hostIP[256];
    char instanceName[256];
    char databaseName[256];
    char dbUserName[128];
    char logicClusterName[128];
    char additionInfo[256];
} AlarmAdditionalParam;

/*total alarm types of alarm module.*/
typedef enum AlarmType { ALM_AT_Fault = 0, ALM_AT_Resume = 2, ALM_AT_OPLog, ALM_AT_Event, ALM_AT_Delete } AlarmType;

/*status of a specific alarm.*/
typedef enum AlarmStat { ALM_AS_Normal, ALM_AS_Reported } AlarmStat;

/*result types of alarm check functions.*/
typedef enum AlarmCheckResult {
    ALM_ACR_UnKnown = 0,

    ALM_ACR_Normal,
    ALM_ACR_Abnormal
} AlarmCheckResult;

typedef enum AlarmModule {
    ALM_AM_NoModule = 0,

    ALM_AM_Coordinator,
    ALM_AM_Datanode,
    ALM_AM_GTM,
    ALM_AM_CMServer
} AlarmModule;

typedef struct Alarm {
    AlarmId id;
    AlarmStat stat;
    time_t lastReportTime;
    long startTimeStamp;
    long endTimeStamp;
    int reportCount;
    char infoEn[256];
    char infoCh[256];

    AlarmCheckResult (*checker)(Alarm* alarm, AlarmAdditionalParam* additionalParam);
} Alarm;

typedef AlarmCheckResult (*CheckerFunc)(Alarm* alarm, AlarmAdditionalParam* additionalParam);

/*common function to check*/
extern void AlarmEnvInitialize(bool enableLogHostname = false);

extern void AlarmItemInitialize(Alarm* alarmItem, AlarmId alarmId, AlarmStat alarmStat, CheckerFunc checkerFunc,
    time_t reportTime = 0, int reportCount = 0);

extern void AlarmCheckerLoop(Alarm* checkList, int checkListSize);

extern void WriteAlarmAdditionalInfo(AlarmAdditionalParam* additionalParam, const char* instanceName,
    const char* databaseName, const char* dbUserName, Alarm* alarmItem, AlarmType type, ...);

extern void WriteAlarmAdditionalInfoForLC(AlarmAdditionalParam* additionalParam, const char* instanceName,
    const char* databaseName, const char* dbUserName, const char* logicClusterName, Alarm* alarmItem, AlarmType type,
    ...);

extern void AlarmReporter(Alarm* alarmItem, AlarmType type, AlarmAdditionalParam* additionalParam);

/*alarm module only log in two level.*/
#define ALM_LOG 1
#define ALM_DEBUG 2
#define AlarmLogPrefix "[Alarm Module]"

#define CLUSTER_NAME_LEN 64

extern void AlarmLog(int level, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

/*below things must be implemented by other modules(gaussdb, gtm, cmserver etc.).*/

/*declare the guc variable of alarm module*/
extern char* Alarm_component;
extern THR_LOCAL int AlarmReportInterval;

/*declare the global variable of alarm module*/
extern int g_alarmReportInterval;
extern int g_alarmReportMaxCount;
extern char g_alarmComponentPath[MAXPGPATH];

/*alarm module memory alloc method maybe different in different module.*/
extern void* AlarmAlloc(size_t size);
extern void AlarmFree(void* pointer);

/*callback protype for get logic cluster name.*/
typedef void (*cb_for_getlc)(char*);
/*set callback function for get logic cluster name*/
void SetcbForGetLCName(cb_for_getlc get_lc_name);

/*alarm log implementation.*/
extern void AlarmLogImplementation(int level, const char* prefix, const char* logtext);

#endif
