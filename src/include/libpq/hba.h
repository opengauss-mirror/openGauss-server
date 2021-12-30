/* -------------------------------------------------------------------------
 *
 * hba.h
 *	  Interface to hba.c
 *
 *
 * src/include/libpq/hba.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HBA_H
#define HBA_H

#include "libpq/pqcomm.h" /* pgrminclude ignore */ /* needed for NetBSD */
#include "nodes/pg_list.h"

typedef enum UserAuth {
    uaReject,
    uaImplicitReject,
    uaKrb5,
    uaTrust,
    uaIdent,
    uaMD5,
    uaSHA256,
    uaGSS,
    uaSSPI,
    uaPAM,
    uaLDAP,
    uaCert,
    uaPeer,
    uaIAM,
    uaSM3
} UserAuth;

typedef enum IPCompareMethod { ipCmpMask, ipCmpSameHost, ipCmpSameNet, ipCmpAll } IPCompareMethod;

typedef enum ConnType { ctLocal, ctHost, ctHostSSL, ctHostNoSSL } ConnType;

typedef struct HbaLine {
    int linenumber;
    ConnType conntype;
    List* databases;
    List* roles;
    struct sockaddr_storage addr;
    struct sockaddr_storage mask;
    IPCompareMethod ip_cmp_method;
    char* hostname;
    UserAuth auth_method;

    char* usermap;
    char* pamservice;
    bool ldaptls;
    char* ldapserver;
    int ldapport;
    char* ldapbinddn;
    char* ldapbindpasswd;
    char* ldapsearchattribute;
    char* ldapbasedn;
    char* ldapprefix;
    char* ldapsuffix;
    bool clientcert;
    char* krb_server_hostname;
    char* krb_realm;
    bool include_realm;
    bool remoteTrust;
} HbaLine;

/* kluge to avoid including libpq/libpq-be.h here */
typedef struct Port hbaPort;

extern bool load_hba(void);
extern void check_old_hba(bool);
extern void load_ident(void);
extern void hba_getauthmethod(hbaPort* port);
extern bool IsLoopBackAddr(Port* port);
extern int check_usermap(const char* usermap_name, const char* pg_role, const char* auth_user, bool case_sensitive);

extern bool pg_isblank(const char c);
extern bool is_cluster_internal_connection(hbaPort* port);
extern bool is_node_internal_connection(hbaPort* port);
extern bool is_cluster_internal_IP(sockaddr peer_addr);
extern bool check_ip_whitelist(hbaPort* port, char* ip, unsigned int ip_len);

#endif /* HBA_H */
