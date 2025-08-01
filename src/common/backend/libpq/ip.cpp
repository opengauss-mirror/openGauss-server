/* -------------------------------------------------------------------------
 *
 * ip.cpp
 *	  IPv6-aware network access.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/libpq/ip.cpp
 *
 * This file and the IPV6 implementation were initially provided by
 * Nigel Kukard <nkukard@lbsd.net>, Linux Based Systems Design
 * http://www.lbsd.net.
 *
 * -------------------------------------------------------------------------
 */

/* This is intended to be used in both frontend and backend, so use c.h */
#include "c.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#include <sys/file.h>

#include "libpq/ip.h"
#ifndef FRONTEND
#include "utils/palloc.h"
#endif

#include "postgres_fe.h"

/* Notice: ip.cpp can not compile and link to kernel(backend).
 * So we can't use securec_check, securec_check_ss and elog.
 */
#define SECUREC_CHECK(rc) securec_check_c(rc, "", "")
#define SECUREC_CHECK_SS(rc) securec_check_ss_c(rc, "", "")

static int range_sockaddr_AF_INET(
    const struct sockaddr_in* addr, const struct sockaddr_in* netaddr, const struct sockaddr_in* netmask);

#ifdef HAVE_IPV6
static int range_sockaddr_AF_INET6(
    const struct sockaddr_in6* addr, const struct sockaddr_in6* netaddr, const struct sockaddr_in6* netmask);
#endif

#ifdef HAVE_UNIX_SOCKETS
static int getaddrinfo_unix(const char* path, const struct addrinfo* hintsp, struct addrinfo** result);

static int getnameinfo_unix(const struct sockaddr_un* sa, int salen, char* node, int nodelen, char* service,
    int servicelen, unsigned int flags);
#endif

int resolveHostname2Ip(int netType, char* hostname, char* ip)
{
#define IP_LEN 64

    struct addrinfo *gaiResult = NULL, *gai = NULL;
    int ret = -1;
    ret = getaddrinfo(hostname, NULL, NULL, &gaiResult);
    if (ret != 0) {
        return ret;
    }

    for (gai = gaiResult; gai; gai = gai->ai_next) {
        if (gai->ai_addr->sa_family == netType) {
            if (gai->ai_addr->sa_family == AF_INET) {
                struct sockaddr_in *h = (struct sockaddr_in *)gai->ai_addr;
                ret = strcpy_s(ip, IP_LEN, inet_ntoa(h->sin_addr));
                SECUREC_CHECK(ret);
                break;
            }
#ifdef HAVE_IPV6
            else if (gai->ai_addr->sa_family == AF_INET6) {
                void *addr;
                struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)gai->ai_addr;
                addr = &(ipv6->sin6_addr);
                inet_ntop(gai->ai_family, addr, ip, IP_LEN);
                break;
            }  
#endif
        }
    }
    if (gaiResult != NULL) {
        freeaddrinfo(gaiResult);
    }
    
    return ret;
}

int resolveHostIp2Name(int netType, char* ip, char* hostname)
{
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    struct sockaddr_in6 addr6;
    addr6.sin6_family = AF_INET6;
    int ret = -1;

    if (netType == AF_INET) {
        if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
            return ret;
        }
        ret = getnameinfo((struct sockaddr *)&addr, sizeof(struct sockaddr_in), hostname, NI_MAXHOST, NULL, 0, NI_NAMEREQD);
    } 
#ifdef HAVE_IPV6
    else {
        if (inet_pton(AF_INET6, ip, &addr6.sin6_addr) != 1) {
            return ret;
        }
        ret = getnameinfo((struct sockaddr *)&addr6, sizeof(struct sockaddr_in6), hostname, NI_MAXHOST, NULL, 0, NI_NAMEREQD);
    }
#endif

    return ret;
}

/*
 *	pg_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 */
int pg_getaddrinfo_all(
    const char* hostname, const char* servname, const struct addrinfo* hintp, struct addrinfo** result)
{
    int rc = -1;

    /* not all versions of getaddrinfo() zero *result on failure */
    *result = NULL;

#ifdef HAVE_UNIX_SOCKETS
    if (hintp->ai_family == AF_UNIX)
        return getaddrinfo_unix(servname, hintp, result);
#endif

    /* NULL has special meaning to getaddrinfo(). */
    rc = getaddrinfo((hostname == NULL || hostname[0] == '\0') ? NULL : hostname, servname, hintp, result);

    return rc;
}

/*
 *	pg_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix
 *
 * Note: the ai_family field of the original hint structure must be passed
 * so that we can tell whether the addrinfo struct was built by the system's
 * getaddrinfo() routine or our own getaddrinfo_unix() routine.  Some versions
 * of getaddrinfo() might be willing to return AF_UNIX addresses, so it's
 * not safe to look at ai_family in the addrinfo itself.
 */
void pg_freeaddrinfo_all(int hint_ai_family, struct addrinfo* ai)
{
#ifdef HAVE_UNIX_SOCKETS
    if (hint_ai_family == AF_UNIX) {
        /* struct was built by getaddrinfo_unix (see pg_getaddrinfo_all) */
        while (ai != NULL) {
            struct addrinfo* p = ai;

            ai = ai->ai_next;
#ifdef FRONTEND
            free(p->ai_addr);
            free(p);
#else
            pfree(p->ai_addr);
            pfree(p);
#endif
        }
    } else
#endif /* HAVE_UNIX_SOCKETS */
    {
        /* struct was built by getaddrinfo() */
        if (ai != NULL) {
            freeaddrinfo(ai);
        }
    }
}

/*
 *	pg_getnameinfo_all - get name info for Unix, IPv4 and IPv6 sockets
 *
 * The API of this routine differs from the standard getnameinfo() definition
 * in two ways: first, the addr parameter is declared as sockaddr_storage
 * rather than struct sockaddr, and second, the node and service fields are
 * guaranteed to be filled with something even on failure return.
 */
int pg_getnameinfo_all(
    const struct sockaddr_storage* addr, int salen, char* node, int nodelen, char* service, int servicelen, int flags)
{
    int rc = -1;

#ifdef HAVE_UNIX_SOCKETS
    if (addr != NULL && addr->ss_family == AF_UNIX)
        rc = getnameinfo_unix((const struct sockaddr_un*)addr, salen, node, nodelen, service, servicelen, flags);
    else
#endif
        rc = getnameinfo((const struct sockaddr*)addr, salen, node, nodelen, service, servicelen, flags);

    if (rc != 0) {
        if (node != NULL)
            strlcpy(node, "\?\?\?", nodelen);
        if (service != NULL)
            strlcpy(service, "\?\?\?", servicelen);
    }

    return rc;
}

#if defined(HAVE_UNIX_SOCKETS)

/* -------
 *	getaddrinfo_unix - get unix socket info using IPv6-compatible API
 *
 *	Bugs: only one addrinfo is set even though hintsp is NULL or
 *		  ai_socktype is 0
 *		  AI_CANONNAME is not supported.
 * -------
 */
static int getaddrinfo_unix(const char* path, const struct addrinfo* hintsp, struct addrinfo** result)
{
    struct addrinfo hints;
    struct addrinfo* aip = NULL;
    struct sockaddr_un* unp = NULL;
    int rcs = 0;

    *result = NULL;

    rcs = memset_s(&hints, sizeof(hints), 0, sizeof(hints));
    SECUREC_CHECK(rcs);

    if (strlen(path) >= sizeof(unp->sun_path))
        return EAI_FAIL;

    if (hintsp == NULL) {
        hints.ai_family = AF_UNIX;
        hints.ai_socktype = SOCK_STREAM;
    } else {
        rcs = memcpy_s(&hints, sizeof(hints), hintsp, sizeof(hints));
        SECUREC_CHECK(rcs);
    }

    if (hints.ai_socktype == 0)
        hints.ai_socktype = SOCK_STREAM;

    if (hints.ai_family != AF_UNIX) {
        /* shouldn't have been called */
        return EAI_FAIL;
    }

#ifdef FRONTEND
    aip = (addrinfo*)calloc(1, sizeof(struct addrinfo));
#else
    aip = (addrinfo*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), 1 * sizeof(struct addrinfo));
#endif
    if (aip == NULL)
        return EAI_MEMORY;

#ifdef FRONTEND
    unp = (sockaddr_un*)calloc(1, sizeof(struct sockaddr_un));
#else
    unp = (sockaddr_un*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_COMMUNICATION), 1 * sizeof(struct sockaddr_un));
#endif
    if (unp == NULL) {
#ifdef FRONTEND
        free(aip);
#else
        pfree(aip);
#endif
        return EAI_MEMORY;
    }

    aip->ai_family = AF_UNIX;
    aip->ai_socktype = hints.ai_socktype;
    aip->ai_protocol = hints.ai_protocol;
    aip->ai_next = NULL;
    aip->ai_canonname = NULL;
    *result = aip;

    unp->sun_family = AF_UNIX;
    aip->ai_addr = (struct sockaddr*)unp;
    aip->ai_addrlen = sizeof(struct sockaddr_un);

    rcs = strcpy_s(unp->sun_path, sizeof(unp->sun_path), path);
    SECUREC_CHECK(rcs);

#ifdef HAVE_STRUCT_SOCKADDR_STORAGE_SS_LEN
    unp->sun_len = sizeof(struct sockaddr_un);
#endif

    return 0;
}

/*
 * Convert an address to a hostname.
 */
static int getnameinfo_unix(
    const struct sockaddr_un* sa, int salen, char* node, int nodelen, char* service, int servicelen, unsigned int flags)
{
    int ret = -1;

    /* Invalid arguments. */
    if (sa == NULL || sa->sun_family != AF_UNIX || (node == NULL && service == NULL)) {
        return EAI_FAIL;
    }

    /* We don't support those. */
    if (((node != NULL) && !(flags & NI_NUMERICHOST)) || ((service != NULL) && !(flags & NI_NUMERICSERV))) {
        return EAI_FAIL;
    }

    if (node != NULL) {
        ret = snprintf_s(node, nodelen, nodelen - 1, "%s", "[local]");
        if (ret == -1) {
            return EAI_MEMORY;
        }
    }

    if (service != NULL) {
        ret = snprintf_s(service, servicelen, servicelen - 1, "%s", sa->sun_path);
        if (ret == -1) {
            return EAI_MEMORY;
        }
    }

    return 0;
}
#endif /* HAVE_UNIX_SOCKETS */

/*
 * pg_range_sockaddr - is addr within the subnet specified by netaddr/netmask ?
 *
 * Note: caller must already have verified that all three addresses are
 * in the same address family; and AF_UNIX addresses are not supported.
 */
int pg_range_sockaddr(
    const struct sockaddr_storage* addr, const struct sockaddr_storage* netaddr, const struct sockaddr_storage* netmask)
{
    if (addr->ss_family == AF_INET) {
        return range_sockaddr_AF_INET(
            (const struct sockaddr_in*)addr, (const struct sockaddr_in*)netaddr, (const struct sockaddr_in*)netmask);
    }
#ifdef HAVE_IPV6
    else if (addr->ss_family == AF_INET6) {
        return range_sockaddr_AF_INET6(
            (const struct sockaddr_in6*)addr, (const struct sockaddr_in6*)netaddr, (const struct sockaddr_in6*)netmask);
    }
#endif
    else {
        return 0;
    }
}

static int range_sockaddr_AF_INET(
    const struct sockaddr_in* addr, const struct sockaddr_in* netaddr, const struct sockaddr_in* netmask)
{
    if (((addr->sin_addr.s_addr ^ netaddr->sin_addr.s_addr) & netmask->sin_addr.s_addr) == 0) {
        return 1;
    } else {
        return 0;
    }
}

#ifdef HAVE_IPV6

static int range_sockaddr_AF_INET6(
    const struct sockaddr_in6* addr, const struct sockaddr_in6* netaddr, const struct sockaddr_in6* netmask)
{
    int i;

    for (i = 0; i < 16; i++) {
        if (((addr->sin6_addr.s6_addr[i] ^ netaddr->sin6_addr.s6_addr[i]) & netmask->sin6_addr.s6_addr[i]) != 0) {
            return 0;
        }
    }

    return 1;
}
#endif /* HAVE_IPV6 */

/*
 *	pg_sockaddr_cidr_mask - make a network mask of the appropriate family
 *	  and required number of significant bits
 *
 * numbits can be null, in which case the mask is fully set.
 *
 * The resulting mask is placed in *mask, which had better be big enough.
 *
 * Return value is 0 if okay, -1 if not.
 */
int pg_sockaddr_cidr_mask(struct sockaddr_storage* mask, const char* numbits, int family)
{
    long bits;
    int rcs = 0;
    char* endptr = NULL;

    if (numbits == NULL) {
        bits = (family == AF_INET) ? 32 : 128;
    } else {
        bits = strtol(numbits, &endptr, 10);
        if (*numbits == '\0' || *endptr != '\0')
            return -1;
    }

    switch (family) {
        case AF_INET: {
            struct sockaddr_in mask4;
            long maskl;

            if (bits < 0 || bits > 32)
                return -1;
            rcs = memset_s(&mask4, sizeof(mask4), 0, sizeof(mask4));
            SECUREC_CHECK(rcs);
            /* avoid "x << 32", which is not portable */
            if (bits > 0)
                maskl = (0xffffffffUL << (32 - (unsigned int)bits)) & 0xffffffffUL;
            else
                maskl = 0;
            mask4.sin_addr.s_addr = htonl(maskl);
            rcs = memcpy_s(mask, sizeof(mask4), &mask4, sizeof(mask4));
            SECUREC_CHECK(rcs);
            break;
        }

#ifdef HAVE_IPV6
        case AF_INET6: {
            struct sockaddr_in6 mask6;
            int i;

            if (bits < 0 || bits > 128)
                return -1;
            rcs = memset_s(&mask6, sizeof(mask6), 0, sizeof(mask6));
            SECUREC_CHECK(rcs);
            for (i = 0; i < 16; i++) {
                if (bits <= 0)
                    mask6.sin6_addr.s6_addr[i] = 0;
                else if (bits >= 8)
                    mask6.sin6_addr.s6_addr[i] = 0xff;
                else {
                    mask6.sin6_addr.s6_addr[i] = (0xff << (8 - (int)bits)) & 0xff;
                }
                bits -= 8;
            }
            rcs = memcpy_s(mask, sizeof(mask6), &mask6, sizeof(mask6));
            SECUREC_CHECK(rcs);
            break;
        }
#endif
        default:
            return -1;
    }

    mask->ss_family = family;
    return 0;
}

#ifdef HAVE_IPV6

/*
 * pg_promote_v4_to_v6_addr --- convert an AF_INET addr to AF_INET6, using
 *		the standard convention for IPv4 addresses mapped into IPv6 world
 *
 * The passed addr is modified in place; be sure it is large enough to
 * hold the result!  Note that we only worry about setting the fields
 * that pg_range_sockaddr will look at.
 */
void pg_promote_v4_to_v6_addr(struct sockaddr_storage* addr)
{
    struct sockaddr_in addr4;
    struct sockaddr_in6 addr6;
    uint32 ip4addr;
    int rcs = 0;

    rcs = memcpy_s(&addr4, sizeof(addr4), addr, sizeof(addr4));
    SECUREC_CHECK(rcs);
    ip4addr = ntohl(addr4.sin_addr.s_addr);

    rcs = memset_s(&addr6, sizeof(addr6), 0, sizeof(addr6));
    SECUREC_CHECK(rcs);

    addr6.sin6_family = AF_INET6;

    addr6.sin6_addr.s6_addr[10] = 0xff;
    addr6.sin6_addr.s6_addr[11] = 0xff;
    addr6.sin6_addr.s6_addr[12] = (ip4addr >> 24) & 0xFF;
    addr6.sin6_addr.s6_addr[13] = (ip4addr >> 16) & 0xFF;
    addr6.sin6_addr.s6_addr[14] = (ip4addr >> 8) & 0xFF;
    addr6.sin6_addr.s6_addr[15] = (ip4addr)&0xFF;

    rcs = memcpy_s(addr, sizeof(addr6), &addr6, sizeof(addr6));
    SECUREC_CHECK(rcs);
}

/*
 * pg_promote_v4_to_v6_mask --- convert an AF_INET netmask to AF_INET6, using
 *		the standard convention for IPv4 addresses mapped into IPv6 world
 *
 * This must be different from pg_promote_v4_to_v6_addr because we want to
 * set the high-order bits to 1's not 0's.
 *
 * The passed addr is modified in place; be sure it is large enough to
 * hold the result!  Note that we only worry about setting the fields
 * that pg_range_sockaddr will look at.
 */
void pg_promote_v4_to_v6_mask(struct sockaddr_storage* addr)
{
    struct sockaddr_in addr4;
    struct sockaddr_in6 addr6;
    uint32 ip4addr;
    int i;
    int rcs = 0;

    rcs = memcpy_s(&addr4, sizeof(addr4), addr, sizeof(addr4));
    SECUREC_CHECK(rcs);
    ip4addr = ntohl(addr4.sin_addr.s_addr);

    rcs = memset_s(&addr6, sizeof(addr6), 0, sizeof(addr6));
    SECUREC_CHECK(rcs);

    addr6.sin6_family = AF_INET6;

    for (i = 0; i < 12; i++)
        addr6.sin6_addr.s6_addr[i] = 0xff;

    addr6.sin6_addr.s6_addr[12] = (ip4addr >> 24) & 0xFF;
    addr6.sin6_addr.s6_addr[13] = (ip4addr >> 16) & 0xFF;
    addr6.sin6_addr.s6_addr[14] = (ip4addr >> 8) & 0xFF;
    addr6.sin6_addr.s6_addr[15] = (ip4addr)&0xFF;

    rcs = memcpy_s(addr, sizeof(addr6), &addr6, sizeof(addr6));
    SECUREC_CHECK(rcs);
}
#endif /* HAVE_IPV6 */

/*
 * Run the callback function for the addr/mask, after making sure the
 * mask is sane for the addr.
 */
static void run_ifaddr_callback(PgIfAddrCallback callback, void* cb_data, struct sockaddr* addr, struct sockaddr* mask)
{
    struct sockaddr_storage fullmask;

    if (addr == NULL) {
        return;
    }

    /* Check that the mask is valid */
    if (mask != NULL) {
        if (mask->sa_family != addr->sa_family) {
            mask = NULL;
        } else if (mask->sa_family == AF_INET) {
            if (((struct sockaddr_in*)mask)->sin_addr.s_addr == INADDR_ANY) {
                mask = NULL;
            }
        }
#ifdef HAVE_IPV6
        else if (mask->sa_family == AF_INET6) {
            if (IN6_IS_ADDR_UNSPECIFIED(&((struct sockaddr_in6*)mask)->sin6_addr)) {
                mask = NULL;
            }
        }
#endif
    }

    /* If mask is invalid, generate our own fully-set mask */
    if (mask == NULL) {
        pg_sockaddr_cidr_mask(&fullmask, NULL, addr->sa_family);
        mask = (struct sockaddr*)&fullmask;
    }

    (*callback)(addr, mask, cb_data);
}

#ifdef WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version is for Win32.  Uses the Winsock 2 functions (ie: ws2_32.dll)
 */
int pg_foreach_ifaddr(PgIfAddrCallback callback, void* cb_data)
{
    INTERFACE_INFO* ptr = NULL;
    INTERFACE_INFO* ii = NULL;

    unsigned long length, i;
    unsigned long n_ii = 0;
    const unsigned long ip_len = 64;
    SOCKET sock = INVALID_SOCKET;
    int error;
    errno_t rc;

    sock = WSASocket(AF_INET, SOCK_DGRAM, 0, 0, 0, 0);
    if (sock == SOCKET_ERROR)
        return -1;

    while (n_ii < 1024) {
        n_ii += ip_len;

#ifdef FRONTEND
#ifdef WIN32
        ptr = (INTERFACE_INFO*)malloc(sizeof(INTERFACE_INFO) * n_ii);
#else
        ptr = (INTERFACE_INFO*)malloc(sizeof(INTERFACE_INFO) * n_ii);
        if (ptr != NULL && ii != NULL) {
            rc = memcpy_s(ptr, sizeof(INTERFACE_INFO) * n_ii, ii, sizeof(INTERFACE_INFO) * (n_ii - ip_len));
            securec_check(rc, "\0", "\0");
            free(ii);
        }
#endif /*WIN32*/
#else
        ptr = repalloc(ii, sizeof(INTERFACE_INFO) * n_ii);
#endif
        if (ptr == NULL) {
            if (ii != NULL) {
#ifdef FRONTEND
                free(ii);
#else
                pfree(ii);
#endif
            }
            /* CommProxy Interface Support */
#ifndef WIN32
            comm_closesocket(sock);
#else
            closesocket(sock);
#endif
            errno = ENOMEM;
            return -1;
        }

        ii = ptr;
        if (WSAIoctl(sock, SIO_GET_INTERFACE_LIST, 0, 0, ii, n_ii * sizeof(INTERFACE_INFO), &length, 0, 0) ==
            SOCKET_ERROR) {
            error = WSAGetLastError();
            if (error == WSAEFAULT || error == WSAENOBUFS)
                continue; /* need to make the buffer bigger */

            /* CommProxy Interface Support */
#ifndef WIN32
            comm_closesocket(sock);
#else
            closesocket(sock);
#endif
#ifdef FRONTEND
            free(ii);
#else
            pfree(ii);
#endif
            return -1;
        }

        break;
    }

    for (i = 0; i < length / sizeof(INTERFACE_INFO); ++i)
        run_ifaddr_callback(callback, cb_data, (struct sockaddr*)&ii[i].iiAddress, (struct sockaddr*)&ii[i].iiNetmask);

    /* CommProxy Interface Support */
#ifndef WIN32
    comm_closesocket(sock);
#else
    closesocket(sock);
#endif
#ifdef FRONTEND
    free(ii);
#else
    pfree(ii);
#endif
    return 0;
}
#elif HAVE_GETIFADDRS /* && !WIN32 */

#ifdef HAVE_IFADDRS_H
#include <ifaddrs.h>
#endif

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version uses the getifaddrs() interface, which is available on
 * BSDs, AIX, and modern Linux.
 */
int pg_foreach_ifaddr(PgIfAddrCallback callback, void* cb_data)
{
    struct ifaddrs *ifa = NULL, *l = NULL;

    if (getifaddrs(&ifa) < 0)
        return -1;

    for (l = ifa; l; l = l->ifa_next)
        run_ifaddr_callback(callback, cb_data, l->ifa_addr, l->ifa_netmask);

    freeifaddrs(ifa);
    return 0;
}
#else /* !HAVE_GETIFADDRS && !WIN32 */

#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif

#ifdef HAVE_NET_IF_H
#include <net/if.h>
#endif

#ifdef HAVE_SYS_SOCKIO_H
#include <sys/sockio.h>
#endif

/*
 * SIOCGIFCONF does not return IPv6 addresses on Solaris
 * and HP/UX. So we prefer SIOCGLIFCONF if it's available.
 *
 * On HP/UX, however, it *only* returns IPv6 addresses,
 * and the structs are named slightly differently too.
 * We'd have to do another call with SIOCGIFCONF to get the
 * IPv4 addresses as well. We don't currently bother, just
 * fall back to SIOCGIFCONF on HP/UX.
 */

#if defined(SIOCGLIFCONF) && !defined(__hpux)

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version uses ioctl(SIOCGLIFCONF).
 */
int pg_foreach_ifaddr(PgIfAddrCallback callback, void* cb_data)
{
    struct lifconf lifc;
    struct lifreq *lifr = NULL, lmask = NULL;
    struct sockaddr *addr = NULL, *mask = NULL;
    char *ptr = NULL,
    char *buffer = NULL;
    size_t n_buffer = 1024;
    pgsocket sock, fd;
    int rcs = 0;

#ifdef HAVE_IPV6
    pgsocket sock6;
#endif
    int i, total;

    /* CommProxy Interface Support */
#ifndef WIN32
    sock = comm_socket(AF_INET, SOCK_DGRAM, 0);
#else
    sock = socket(AF_INET, SOCK_DGRAM, 0);
#endif
    if (sock == -1)
        return -1;

    while (n_buffer < 1024 * 100) {
        n_buffer += 1024;

#ifdef FRONTEND
#ifdef __sparc
        ptr = (char*)realloc(buffer, n_buffer);
#else
        ptr = realloc(buffer, n_buffer);
#endif /*sparc*/
#else
        ptr = repalloc(buffer, n_buffer);
#endif
        if (ptr == NULL) {
#ifdef FRONTEND
            free(buffer);
#else
            pfree(buffer);
#endif
            /* CommProxy Interface Support */
#ifndef WIN32
            comm_close(sock);
#else
            close(sock);
#endif
            errno = ENOMEM;
            return -1;
        }

        rcs = memset_s(&lifc, sizeof(lifc), 0, sizeof(lifc));
        SECUREC_CHECK(rcs);
        lifc.lifc_family = AF_UNSPEC;
        lifc.lifc_buf = buffer = ptr;
        lifc.lifc_len = n_buffer;

        if (ioctl(sock, SIOCGLIFCONF, &lifc) < 0) {
            if (errno == EINVAL)
                continue;

#ifdef FRONTEND
            free(buffer);
#else
            pfree(buffer);
#endif
            /* CommProxy Interface Support */
#ifndef WIN32
            comm_close(sock);
#else
            close(sock);
#endif
            return -1;
        }

        /*
         * Some Unixes try to return as much data as possible, with no
         * indication of whether enough space allocated. Don't believe we have
         * it all unless there's lots of slop.
         */
        if ((size_t)lifc.lifc_len < n_buffer - 1024)
            break;
    }

#ifdef HAVE_IPV6
    /* We'll need an IPv6 socket too for the SIOCGLIFNETMASK ioctls */
    /* CommProxy Interface Support */
#ifndef WIN32
    sock6 = comm_socket(AF_INET6, SOCK_DGRAM, 0);
#else
    sock6 = socket(AF_INET6, SOCK_DGRAM, 0);
#endif
    if (sock6 == -1) {
#ifdef FRONTEND
        free(buffer);
#else
        pfree(buffer);
#endif
        /* CommProxy Interface Support */
#ifndef WIN32
        comm_close(sock);
#else
        close(sock);
#endif
        return -1;
    }
#endif

    total = lifc.lifc_len / sizeof(struct lifreq);
    lifr = lifc.lifc_req;
    for (i = 0; i < total; ++i) {
        addr = (struct sockaddr*)&lifr[i].lifr_addr;
        rcs = memcpy_s(&lmask, sizeof(struct lifreq), &lifr[i], sizeof(struct lifreq));
        SECUREC_CHECK(rcs);
#ifdef HAVE_IPV6
        fd = (addr->sa_family == AF_INET6) ? sock6 : sock;
#else
        fd = sock;
#endif
        if (ioctl(fd, SIOCGLIFNETMASK, &lmask) < 0)
            mask = NULL;
        else
            mask = (struct sockaddr*)&lmask.lifr_addr;
        run_ifaddr_callback(callback, cb_data, addr, mask);
    }

#ifdef FRONTEND
    free(buffer);
#else
    pfree(buffer);
#endif

#ifndef WIN32
    comm_close(sock);  /* CommProxy Interface Support */
#else
    close(sock);
#endif
#ifdef HAVE_IPV6
#ifndef WIN32
    comm_close(sock6); /* CommProxy Interface Support */
#else
    close(sock);
#endif
#endif
    return 0;
}
#elif defined(SIOCGIFCONF)

/*
 * Remaining Unixes use SIOCGIFCONF. Some only return IPv4 information
 * here, so this is the least preferred method. Note that there is no
 * standard way to iterate the struct ifreq returned in the array.
 * On some OSs the structures are padded large enough for any address,
 * on others you have to calculate the size of the struct ifreq.
 */

/* Some OSs have _SIZEOF_ADDR_IFREQ, so just use that */
#ifndef _SIZEOF_ADDR_IFREQ

/* Calculate based on sockaddr.sa_len */
#ifdef HAVE_STRUCT_SOCKADDR_SA_LEN
#define _SIZEOF_ADDR_IFREQ(ifr)                                                        \
    ((ifr).ifr_addr.sa_len > sizeof(struct sockaddr)                                   \
            ? (sizeof(struct ifreq) - sizeof(struct sockaddr) + (ifr).ifr_addr.sa_len) \
            : sizeof(struct ifreq))

/* Padded ifreq structure, simple */
#else
#define _SIZEOF_ADDR_IFREQ(ifr) sizeof(struct ifreq)
#endif
#endif /* !_SIZEOF_ADDR_IFREQ */

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version uses ioctl(SIOCGIFCONF).
 */
int pg_foreach_ifaddr(PgIfAddrCallback callback, void* cb_data)
{
    struct ifconf ifc;
    struct ifreq *ifr = NULL, *end = NULL, addr, mask;
    char *ptr = NULL, *buffer = NULL;
    size_t n_buffer = 1024;
    int sock;
    int rcs = 0;
#ifndef WIN32
    sock = comm_socket(AF_INET, SOCK_DGRAM, 0); /* CommProxy Interface Support */
#else
    sock = socket(AF_INET, SOCK_DGRAM, 0);
#endif
    if (sock == -1)
        return -1;

    while (n_buffer < 1024 * 100) {
        n_buffer += 1024;

#ifdef FRONTEND
        ptr = realloc(buffer, n_buffer);
#else
        ptr = repalloc(buffer, n_buffer);
#endif
        if (ptr == NULL) {
#ifdef FRONTEND
            free(buffer);
#else
            pfree(buffer);
#endif
#ifndef WIN32
            comm_close(sock);  /* CommProxy Interface Support */
#else
            close(sock);
#endif
            errno = ENOMEM;
            return -1;
        }

        rcs = memset_s(&ifc, sizeof(ifc), 0, sizeof(ifc));
        SECUREC_CHECK(rcs);
        ifc.ifc_buf = buffer = ptr;
        ifc.ifc_len = n_buffer;

        if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
            if (errno == EINVAL)
                continue;

#ifdef FRONTEND
            free(buffer);
#else
            pfree(buffer);
#endif
#ifndef WIN32
            /* CommProxy Interface Support */
            comm_close(sock);  
#else
            close(sock);
#endif
            return -1;
        }

        /*
         * Some Unixes try to return as much data as possible, with no
         * indication of whether enough space allocated. Don't believe we have
         * it all unless there's lots of slop.
         */
        if (ifc.ifc_len < n_buffer - 1024)
            break;
    }

    end = (struct ifreq*)(buffer + ifc.ifc_len);
    for (ifr = ifc.ifc_req; ifr < end;) {
        rcs = memcpy_s(&addr, sizeof(addr), ifr, sizeof(addr));
        SECUREC_CHECK(rcs);
        rcs = memcpy_s(&mask, sizeof(mask), ifr, sizeof(mask));
        SECUREC_CHECK(rcs);
        if (ioctl(sock, SIOCGIFADDR, &addr, sizeof(addr)) == 0 && ioctl(sock, SIOCGIFNETMASK, &mask, sizeof(mask)) == 0)
            run_ifaddr_callback(callback, cb_data, &addr.ifr_addr, &mask.ifr_addr);
        ifr = (struct ifreq*)((char*)ifr + _SIZEOF_ADDR_IFREQ(*ifr));
    }

#ifdef FRONTEND
    free(buffer);
#else
    pfree(buffer);
#endif
#ifndef WIN32
    comm_close(sock);  /* CommProxy Interface Support */
#else
    close(sock);
#endif
    return 0;
}
#else /* !defined(SIOCGIFCONF) */

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version is our fallback if there's no known way to get the
 * interface addresses.  Just return the standard loopback addresses.
 */
int pg_foreach_ifaddr(PgIfAddrCallback callback, void* cb_data)
{
    struct sockaddr_in addr;
    struct sockaddr_storage mask;
    int rcs = 0;

#ifdef HAVE_IPV6
    struct sockaddr_in6 addr6;
#endif

    /* addr 127.0.0.1/8 */
    rcs = memset_s(&addr, sizeof(addr) 0, sizeof(addr));
    SECUREC_CHECK(rcs);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ntohl(0x7f000001);
    rcs = memset_s(&mask, sizeof(mask), 0, sizeof(mask));
    SECUREC_CHECK(rcs);
    pg_sockaddr_cidr_mask(&mask, "8", AF_INET);
    run_ifaddr_callback(callback, cb_data, (struct sockaddr*)&addr, (struct sockaddr*)&mask);

#ifdef HAVE_IPV6
    /* addr ::1/128 */
    rcs = memset_s(&addr6, sizeof(addr6), 0, sizeof(addr6));
    SECUREC_CHECK(rcs);
    addr6.sin6_family = AF_INET6;
    addr6.sin6_addr.s6_addr[15] = 1;
    rcs = memset_s(&mask, sizeof(mask), 0, sizeof(mask));
    SECUREC_CHECK(rcs);
    pg_sockaddr_cidr_mask(&mask, "128", AF_INET6);
    run_ifaddr_callback(callback, cb_data, (struct sockaddr*)&addr6, (struct sockaddr*)&mask);
#endif

    return 0;
}
#endif /* !defined(SIOCGIFCONF) */

#endif /* !HAVE_GETIFADDRS */
