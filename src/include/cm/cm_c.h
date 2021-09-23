/* ---------------------------------------------------------------------------------------
 * 
 * cm_c.h
 *        Fundamental C definitions.  This is included by every .c file in
 *	      openGauss (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	      Note that the definitions here are not intended to be exposed to clients
 *	      of the frontend interface libraries --- so we don't worry much about
 *	      polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/cm_c.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_C_H
#define CM_C_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <sys/types.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include "c.h"
#include "securec.h"
#include "securec_check.h"
#include "utils/syscall_lock.h"
#include "cm/etcdapi.h"

#ifdef PC_LINT
#ifndef Assert
#define Assert(condition)       \
    do {                        \
        if (!(bool)(condition)) \
            exit(1);            \
    } while (0)
#endif /* Assert */
#else
#ifndef USE_ASSERT_CHECKING
#ifndef Assert
#define Assert(condition)
#endif /* Assert */
#else
#ifdef Assert
#undef Assert
#endif
#ifndef Assert
#define Assert(condition) assert(condition)
#endif /* Assert */
#endif
#endif
/* Possible type of nodes for registration */
typedef enum CM_PGXCNodeType {
    CM_NODE_GTM_PROXY = 1,
    CM_NODE_GTM_PROXY_POSTMASTER = 2,
    /* Used by Proxy to communicate with GTM and not use Proxy headers */
    CM_NODE_COORDINATOR = 3,
    CM_NODE_DATANODE = 4,
    CM_NODE_GTM = 5,
    CM_NODE_DEFAULT = 6 /* In case nothing is associated to connection */
} CM_PGXCNodeType;

typedef enum CM_PortLastCall {
    CM_LastCall_NONE = 0,
    CM_LastCall_SEND,
    CM_LastCall_RECV,
    CM_LastCall_READ,
    CM_LastCall_WRITE
} CM_PortLastCall;

#define TCP_SOCKET_ERROR_EPIPE (-2)
#define TCP_SOCKET_ERROR_NO_MESSAGE (-3)
#define TCP_SOCKET_ERROR_NO_BUFFER (-4)
#define TCP_SOCKET_ERROR_INVALID_IP (-5)

/* Define max size of user in start up packet */
#define SP_USER 32

/* Define max size of node name in start up packet */
#define SP_NODE_NAME 64
#define MAX_PATH_LEN 1024

/* Define max size of host in start up packet */
#define SP_HOST 16

typedef struct CM_StartupPacket {
    char sp_user[SP_USER];
    char sp_node_name[SP_NODE_NAME];
    char sp_host[SP_HOST];
    int node_id;
    int sp_remotetype;
    bool sp_ispostmaster;
} CM_StartupPacket;

#define _(x) gettext(x)

#define MAX_NODE_GROUP_MEMBERS_LEN (CM_NODE_MAXNUM * LOGIC_DN_PER_NODE * (CM_NODE_NAME + 1))

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr)) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)
#endif

#endif /* GTM_C_H */
