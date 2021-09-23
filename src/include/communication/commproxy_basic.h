/*-------------------------------------------------------------------------
 *
 * comm_basic.h
 *      api for socket programming
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/communication/comm_basic.h
 *
 * NOTES
 *      Libnet or default tcp socket programming
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMPROXY_BASIC_H
#define COMMPROXY_BASIC_H

typedef enum phy_proto_type_t {
    /* Indicate the phy connection is invalid */
    phy_proto_invalid = 0,

    /* Indicate the phy connection is tcp */
    phy_proto_tcp,

    phy_proto_libnet,

    /* Indicate the phy connection is sctp */
    phy_proto_sctp,

    /* Indicate the phy connection is rdma */
    phy_proto_rdma,

    phy_proto_proxy,

    phy_proto_resv,

    /* now, we support max proto type is 14 */
    phy_proto_max = 15

} phy_proto_type_t;

typedef unsigned int CommConnAttr;
typedef enum CommConnAttrType {
    CommConnAttrTypePure = 0,
    CommConnAttrTypeProxy
} CommConnAttrType;
typedef phy_proto_type_t CommConnAttrProto;
/* max support conn proto is (2^4 - 1 - 1) = 14 */
#define CommConnAttrTypeOffset 4
#define SetCommConnAttr(t, p) ((t) << CommConnAttrTypeOffset | (p))
#define CommConnProxyMask (CommConnAttrTypeProxy << CommConnAttrTypeOffset)
#define AmICommConnProxy(x) (((x) & CommConnProxyMask) > 0)
#define CommConnPrueLibnetMask ((CommConnAttrTypePure << CommConnAttrTypeOffset) | phy_proto_libnet)
#define AmICommConnPrueLibNet(x) (((x) & CommConnPrueLibnetMask) > 0)

typedef enum CommConnSocketRole {
    CommConnSocketServer,
    CommConnSockerClient,
    CommConnSockerEpoll
} CommConnSocketRole;

/* now, we support max 4p(8 numa die) node */
typedef unsigned char CommConnNumaMap;
#define CommConnNumaAll(n) ((n) = ~0)
#define CommConnNumaSet(n, x) ((n) = (x))
#define CommConnNumaSingle(n, x) ((n) = (0 | (1) << (x)))
#define CommConnNumaMulti(n, x) ((n) = (n) | (x))
#define CommConnNumaAddNode(n, x) ((n) = (n) | (1) << (x))
#define CommConnNumaDelNode(n, x) ((n) = (n) & ~(1 << (x)))
typedef struct CommSocketOption {
    bool valid;
    CommConnAttr conn_attr;
    CommConnSocketRole conn_role;
    CommConnNumaMap numa_map;
    int group_id;
} CommSocketOption;

typedef enum CommEpollType {
    CommEpollThreadPoolListener = 1,
    CommEpollProxyThread,
    CommEpollPureConnect,
    CommEpollLibcomm
} CommEpollType;
typedef struct CommEpollOption {
    bool valid;
    CommEpollType epoll_type;
    /* if epoll is pure connect, we need group id to select protocol */
    int group_id;
} CommEpollOption;

typedef struct CommPollOption {
    bool valid;
    bool no_proxy;
    bool ctx_switch;
    int threshold;
} CommPollOption;

extern CommSocketOption g_default_invalid_sock_opt;
extern CommEpollOption g_default_invalid_epoll_opt;
extern CommPollOption g_default_invalid_poll_opt;

#define MAX_COMM_PROXY_GROUP_CNT 4
#define MAX_COMM_PURE_GROUP_CNT 8

typedef enum EpollEventType {
    EpollEventOut,
    EpollEventIn
} EpollEventType;

#endif /* COMMPROXY_BASIC_H */
