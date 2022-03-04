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
 * postinit.h
 *        init openGauss thread.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/postinit.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_POSTINIT_H
#define UTILS_POSTINIT_H

#define PGAUDIT_MAXLENGTH 1024

/* --------------------------------
 * openGauss Initialize POSTGRES.
 *
 * The database can be specified by name, using the in_dbname parameter, or by
 * OID, using the dboid parameter.	In the latter case, the actual database
 * name can be returned to the caller in out_dbname.  If out_dbname isn't
 * NULL, it must point to a buffer of size NAMEDATALEN.
 *
 * In bootstrap mode no parameters are used.  The autovacuum launcher process
 * doesn't use any parameters either, because it only goes far enough to be
 * able to read pg_database; it doesn't connect to any particular database.
 * In walsender mode only username is used.
 *
 * We expect InitProcess() was already called, so we already have a PGPROC struct ...
 * but it's not completely filled in yet.
 *
 * Note:
 *		Be very careful with the order of calls in the InitPostgres function.
 * --------------------------------
 */

class PostgresInitializer : public BaseObject {
public:
    PostgresInitializer();

    ~PostgresInitializer();

    void SetDatabaseAndUser(const char* in_dbname, Oid dboid, const char* username, Oid useroid = InvalidOid);

    void GetDatabaseName(char* out_dbname);

    void InitBootstrap();

    void InitJobScheduler();

    void InitJobExecuteWorker();

    void InitSnapshotWorker();

    void InitAspWorker();

    void InitStatementWorker();

    void InitPercentileWorker();

    void InitAutoVacLauncher();

    void InitAutoVacWorker();

    void InitCsnminSync();

    void InitTxnSnapCapturer();

    void InitTxnSnapWorker();

    void InitRbCleaner();

    void InitRbWorker();

    void InitCatchupWorker();

    void InitStreamWorker();

    void InitBgWorker();

    void InitBackendWorker();

    void InitWLM();

    void InitWAL();

    void InitParallelDecode();

    void InitSession();

    void InitStreamingBackend();

    void InitCompactionWorker();

    void InitCompactionWorkerSwitchSession();

    void InitStreamSession();

    void InitUndoLauncher();

    void InitUndoWorker();

    void InitBarrierCreator();

    void InitFencedSysCache();

    void InitLoadLocalSysCache(Oid db_oid, const char *db_name);

    void InitApplyLauncher();

    void InitApplyWorker();

public:
    const char* m_indbname;

    Oid m_dboid;

    const char* m_username;

    Oid m_useroid;

private:
    void InitThread();

    void InitSysCache();

    void SetProcessExitCallback();

    void StartXact();

    void CheckAuthentication();

    void SetSuperUserStandalone();

    void SetSuperUserAndDatabase();

    void CheckAtLeastOneRoles();

    void InitUser();

    void CheckConnPermission();

    void CheckConnPermissionInShutDown();

    void CheckConnPermissionInBinaryUpgrade();

    void CheckConnLimitation();

    void InitPlainWalSender();

    void SetDefaultDatabase();

    void SetFencedMasterDatabase();

    void SetDatabase();

    void SetDatabaseByName();

    void SetDatabaseByOid();

    void LockDatabase();

    void RecheckDatabaseExists();

    void SetDatabasePath();

    void LoadSysCache();

    void ProcessStartupOpt();

    void InitDatabase();

    void InitPGXCPort();

    void InitSettings();

    void InitExtensionVariable();

    void FinishInit();

    void AuditUserLogin();

    void InitCompactionThread();

private:
    bool m_isSuperUser;

    char m_dbname[NAMEDATALEN];

    char m_details[PGAUDIT_MAXLENGTH];

    char* m_fullpath;
};

void ShutdownPostgres(int code, Datum arg);

#endif /* UTILS_POSTINIT_H */
