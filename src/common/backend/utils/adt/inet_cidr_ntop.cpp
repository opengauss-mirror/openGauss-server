/*
 * Copyright (c) 2004 by Internet Systems Consortium, Inc. ("ISC")
 * Copyright (c) 1996,1999 by Internet Software Consortium.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND ISC DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS.  IN NO EVENT SHALL ISC BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *	  src/backend/utils/adt/inet_net_ntop.c
 */

#if defined(LIBC_SCCS) && !defined(lint)
static const char rcsid[] = "Id: inet_net_ntop.c,v 1.1.2.2 2004/03/09 09:17:27 marka Exp $";
#endif

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "utils/builtins.h"
#include "utils/inet.h"

static char* inet_cidr_ntop_ipv4(const u_char* src, int bits, char* dst, size_t size);
static char* inet_cidr_ntop_ipv6(const u_char* src, int bits, char* dst, size_t size);

/*
 * char *
 * inet_cidr_ntop(af, src, bits, dst, size)
 *	convert network number from network to presentation format.
 *	generates CIDR style result always.
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 */
char* inet_cidr_ntop(int af, const void* src, int bits, char* dst, size_t size)
{
    switch (af) {
        case PGSQL_AF_INET:
            return (inet_cidr_ntop_ipv4((const u_char*)src, bits, dst, size));
        case PGSQL_AF_INET6:
            return (inet_cidr_ntop_ipv6((const u_char*)src, bits, dst, size));
        default:
            errno = EAFNOSUPPORT;
            return (NULL);
    }
}

/*
 * static char *
 * inet_cidr_ntop_ipv4(src, bits, dst, size)
 *	convert IPv4 network number from network to presentation format.
 *	generates CIDR style result always.
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0b11110000 in its fourth octet.
 */
static char* inet_cidr_ntop_ipv4(const u_char* src, int bits, char* dst, size_t size)
{
    char* odst = dst;
    char* t = NULL;
    u_int m;
    int b;
    errno_t rc;

    if (bits < 0 || bits > 32) {
        errno = EINVAL;
        return (NULL);
    }

    if (bits == 0) {
        if (size < sizeof "0") {
            goto emsgsize;
        }
        *dst++ = '0';
        size--;
        *dst = '\0';
    }

    /* Format whole octets. */
    for (b = bits / 8; b > 0; b--) {
        if (size <= sizeof "255.") {
            goto emsgsize;
        }
        t = dst;
        rc = sprintf_s(dst, size, "%u", *src++);
        securec_check_ss(rc, "\0", "\0");
        dst += rc;
        if (b > 1) {
            *dst++ = '.';
            *dst = '\0';
        }
        size -= (size_t)(dst - t);
    }

    /* Format partial octet. */
    b = bits % 8;
    if (b > 0) {
        if (size <= sizeof ".255") {
            goto emsgsize;
        }
        t = dst;
        if (dst != odst) {
            *dst++ = '.';
        }
        m = ((1 << (unsigned int)b) - 1) << (8 - (unsigned int)b);
        rc = sprintf_s(dst, size, "%u", *src & m);
        securec_check_ss(rc, "\0", "\0");
        dst += rc;
        size -= (size_t)(dst - t);
    }

    /* Format CIDR /width. */
    if (size <= sizeof "/32") {
        goto emsgsize;
    }
    rc = sprintf_s(dst, size, "/%d", bits);
    securec_check_ss(rc, "\0", "\0");
    dst += rc;
    return (odst);

emsgsize:
    errno = EMSGSIZE;
    return (NULL);
}

/*
 * static char *
 * inet_cidr_ntop_ipv6(src, bits, fakebits, dst, size)
 *	convert IPv6 network number from network to presentation format.
 *	generates CIDR style result always. Picks the shortest representation
 *	unless the IP is really IPv4.
 *	always prints specified number of bits (bits).
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0x11110000 in its fourth octet.
 */

static char* inet_cidr_ntop_ipv6(const u_char* src, int bits, char* dst, size_t size)
{
    u_int m;
    int b;
    int p;
    int zero_s, zero_l, tmp_zero_s, tmp_zero_l;
    int i;
    int is_ipv4 = 0;
    unsigned char inbuf[16];
    char outbuf[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
    char* cp = NULL;
    int words;
    u_char* s = NULL;
    errno_t rc;

    if (bits < 0 || bits > 128) {
        errno = EINVAL;
        return (NULL);
    }

    cp = outbuf;

    if (bits == 0) {
        *cp++ = ':';
        *cp++ = ':';
        *cp = '\0';
    } else {
        /* Copy src to private buffer.	Zero host part. */
        p = (bits + 7) / 8;
        rc = memcpy_s(inbuf, p, src, p);
        securec_check_c(rc, "\0", "\0");
        if (p < 16) {
            rc = memset_s(inbuf + p, 16 - p, 0, 16 - p);
            securec_check_c(rc, "\0", "\0");
        }
        b = bits % 8;
        if (b != 0) {
            m = ~0 << (unsigned int)(8 - b);
            inbuf[p - 1] &= m;
        }

        s = inbuf;

        /* how many words need to be displayed in output */
        words = (bits + 15) / 16;
        if (words == 1)
            words = 2;

        /* Find the longest substring of zero's */
        zero_s = zero_l = tmp_zero_s = tmp_zero_l = 0;
        for (i = 0; i < (words * 2); i += 2) {
            if ((s[i] | s[i + 1]) == 0) {
                if (tmp_zero_l == 0)
                    tmp_zero_s = i / 2;
                tmp_zero_l++;
            } else {
                if (tmp_zero_l && zero_l < tmp_zero_l) {
                    zero_s = tmp_zero_s;
                    zero_l = tmp_zero_l;
                    tmp_zero_l = 0;
                }
            }
        }

        if (tmp_zero_l && zero_l < tmp_zero_l) {
            zero_s = tmp_zero_s;
            zero_l = tmp_zero_l;
        }

        if (zero_l != words && zero_s == 0 &&
            ((zero_l == 6) ||
                ((zero_l == 5 && s[10] == 0xff && s[11] == 0xff) || ((zero_l == 7 && s[14] != 0 && s[15] != 1))))) {
            is_ipv4 = 1;
        }

        /* Format whole words. */
        for (p = 0; p < words; p++) {
            if (zero_l != 0 && p >= zero_s && p < zero_s + zero_l) {
                /* Time to skip some zeros */
                if (p == zero_s) {
                    *cp++ = ':';
                }
                if (p == words - 1) {
                    *cp++ = ':';
                }
                s++;
                s++;
                continue;
            }

            if (is_ipv4 && p > 5) {
                *cp++ = (p == 6) ? ':' : '.';
                rc = sprintf_s(cp, size, "%u", *s++);
                securec_check_ss(rc, "\0", "\0");
                cp += rc;
                /* we can potentially drop the last octet */
                if (p != 7 || bits > 120) {
                    *cp++ = '.';
                    rc = sprintf_s(cp, size, "%u", *s++);
                    securec_check_ss(rc, "\0", "\0");
                    cp += rc;
                }
            } else {
                if (cp != outbuf)
                    *cp++ = ':';
                rc = sprintf_s(cp, size, "%x", *s * 256 + s[1]);
                securec_check_ss(rc, "\0", "\0");
                cp += rc;

                s += 2;
            }
        }
    }
    /* Format CIDR /width. */
    rc = sprintf_s(cp, size, "/%d", bits);
    securec_check_ss(rc, "\0", "\0");

    if (strlen(outbuf) + 1 > size)
        goto emsgsize;
    rc = strcpy_s(dst, size, outbuf);
    securec_check(rc, "\0", "\0");

    return (dst);

emsgsize:
    errno = EMSGSIZE;
    return (NULL);
}
