/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.h
 *	  prototypes for subscriptioncmds.c.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/subscriptioncmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SUBSCRIPTIONCMDS_H
#define SUBSCRIPTIONCMDS_H

#include "nodes/parsenodes.h"

typedef struct HostPort {
    char* host;
    char* port;
} HostPort;

extern ObjectAddress CreateSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel);
extern ObjectAddress AlterSubscription(AlterSubscriptionStmt *stmt);
extern void DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel);

extern ObjectAddress AlterSubscriptionOwner(const char *name, Oid newOwnerId);
extern void AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId);
extern void RenameSubscription(List* oldname, const char* newname);
extern void AddStandbysInfo(char* standbysInfo);
extern void DropStandbysInfo(char* standbysInfo);

extern void ParseConninfo(const char* conninfo, StringInfoData* conninfoWithoutHostPort, HostPort** hostPortList);
extern char* EncryptOrDecryptConninfo(const char* conninfo, const char action);
extern bool AttemptConnectPublisher(const char *conninfoOriginal, char* slotname, bool checkRemoteMode);

#endif							/* SUBSCRIPTIONCMDS_H */

