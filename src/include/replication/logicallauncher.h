/* -------------------------------------------------------------------------
 *
 * logicallauncher.h
 * 	  Exports for logical replication launcher.
 *
 * Portions Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * src/include/replication/logicallauncher.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LOGICALLAUNCHER_H
#define LOGICALLAUNCHER_H

extern void ApplyLauncherRegister(void);
extern void ApplyLauncherMain();

extern Size ApplyLauncherShmemSize(void);
extern void ApplyLauncherShmemInit(void);

extern void ApplyLauncherWakeupAtCommit(void);
extern void AtEOXact_ApplyLauncher(bool isCommit);

extern bool IsLogicalLauncher(void);

#endif /* LOGICALLAUNCHER_H */
