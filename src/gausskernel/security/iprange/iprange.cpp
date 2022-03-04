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
 * iprange.cpp
 *        operation functions for ip data type, which will be used for auditing and masking
 * 
 * 
 * IDENTIFICATION
 *        src/gausskernel/security/iprange/iprange.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include "securec.h"
#include "securec_check.h"
#include "iprange.h"
#include "utils/elog.h"

using namespace std;

#define IPRANGE_IS_IPV4(ip) ((ip).ip_32.b == 0x0000FFFF)

std::vector<uint32_t> MASK_FROM_LUT {
    (uint32_t)0xFFFFFFFF,
    (uint32_t)0xFFFFFFFF << 31,
    (uint32_t)0xFFFFFFFF << 30,
    (uint32_t)0xFFFFFFFF << 29,
    (uint32_t)0xFFFFFFFF << 28,
    (uint32_t)0xFFFFFFFF << 27,
    (uint32_t)0xFFFFFFFF << 26,
    (uint32_t)0xFFFFFFFF << 25,
    (uint32_t)0xFFFFFFFF << 24,
    (uint32_t)0xFFFFFFFF << 23,
    (uint32_t)0xFFFFFFFF << 22,
    (uint32_t)0xFFFFFFFF << 21,
    (uint32_t)0xFFFFFFFF << 20,
    (uint32_t)0xFFFFFFFF << 19,
    (uint32_t)0xFFFFFFFF << 18,
    (uint32_t)0xFFFFFFFF << 17,
    (uint32_t)0xFFFFFFFF << 16,
    (uint32_t)0xFFFFFFFF << 15,
    (uint32_t)0xFFFFFFFF << 14,
    (uint32_t)0xFFFFFFFF << 13,
    (uint32_t)0xFFFFFFFF << 12,
    (uint32_t)0xFFFFFFFF << 11,
    (uint32_t)0xFFFFFFFF << 10,
    (uint32_t)0xFFFFFFFF << 9,
    (uint32_t)0xFFFFFFFF << 8,
    (uint32_t)0xFFFFFFFF << 7,
    (uint32_t)0xFFFFFFFF << 6,
    (uint32_t)0xFFFFFFFF << 5,
    (uint32_t)0xFFFFFFFF << 4,
    (uint32_t)0xFFFFFFFF << 3,
    (uint32_t)0xFFFFFFFF << 2,
    (uint32_t)0xFFFFFFFF << 1,
    (uint32_t)0xFFFFFFFF,
};

std::vector<uint32_t> MASK_TO_LUT {
    (uint32_t)0,
    ~((uint32_t)0xFFFFFFFF << 31),
    ~((uint32_t)0xFFFFFFFF << 30),
    ~((uint32_t)0xFFFFFFFF << 29),
    ~((uint32_t)0xFFFFFFFF << 28),
    ~((uint32_t)0xFFFFFFFF << 27),
    ~((uint32_t)0xFFFFFFFF << 26),
    ~((uint32_t)0xFFFFFFFF << 25),
    ~((uint32_t)0xFFFFFFFF << 24),
    ~((uint32_t)0xFFFFFFFF << 23),
    ~((uint32_t)0xFFFFFFFF << 22),
    ~((uint32_t)0xFFFFFFFF << 21),
    ~((uint32_t)0xFFFFFFFF << 20),
    ~((uint32_t)0xFFFFFFFF << 19),
    ~((uint32_t)0xFFFFFFFF << 18),
    ~((uint32_t)0xFFFFFFFF << 17),
    ~((uint32_t)0xFFFFFFFF << 16),
    ~((uint32_t)0xFFFFFFFF << 15),
    ~((uint32_t)0xFFFFFFFF << 14),
    ~((uint32_t)0xFFFFFFFF << 13),
    ~((uint32_t)0xFFFFFFFF << 12),
    ~((uint32_t)0xFFFFFFFF << 11),
    ~((uint32_t)0xFFFFFFFF << 10),
    ~((uint32_t)0xFFFFFFFF << 9),
    ~((uint32_t)0xFFFFFFFF << 8),
    ~((uint32_t)0xFFFFFFFF << 7),
    ~((uint32_t)0xFFFFFFFF << 6),
    ~((uint32_t)0xFFFFFFFF << 5),
    ~((uint32_t)0xFFFFFFFF << 4),
    ~((uint32_t)0xFFFFFFFF << 3),
    ~((uint32_t)0xFFFFFFFF << 2),
    ~((uint32_t)0xFFFFFFFF << 1),
    (uint32_t)0xFFFFFFFF
};

const IPV6 localhost_ipv6 ({0x0000000000000001, 0x0000000000000000});
const IPV6 localhost_ipv4 ({0xffff7f000001, 0x0});

#define IP_MAX_LEN 128

IPRange::IPRange()
{
}

IPRange::~IPRange()
{
}

void IPRange::net_ipv6_to_host_order(IPV6 *ip, const struct sockaddr_in6 *sa) const
{
    IPV6 tmp_ip;
    int rc = memcpy_s(&(tmp_ip.ip_64), sizeof(tmp_ip.ip_64), &(sa->sin6_addr), sizeof(tmp_ip.ip_64));
    securec_check(rc, "\0", "\0");
    ip->ip_32.a = ntohl(tmp_ip.ip_32.d);
    ip->ip_32.b = ntohl(tmp_ip.ip_32.c);
    ip->ip_32.c = ntohl(tmp_ip.ip_32.b);
    ip->ip_32.d = ntohl(tmp_ip.ip_32.a);
}

void IPRange::net_ipv4_to_host_order(IPV6 *ip, const struct in_addr *addr) const
{
    ip->ip_32.a = ntohl(addr->s_addr);
    ip->ip_32.b = 0x0000FFFF;
    ip->ip_32.c = ip->ip_32.d = 0;
}

bool IPRange::str_to_ip(const char* ip_str, IPV6 *ip)
{
    struct in_addr addr;
    struct sockaddr_in6 sa;

    if (inet_pton(AF_INET6, ip_str, &sa.sin6_addr) > 0) {
        net_ipv6_to_host_order(ip, &sa);
    } else if (inet_pton(AF_INET, ip_str, &addr) > 0) {
        net_ipv4_to_host_order(ip, &addr);
    } else {
        /* 
         * Note that even the format keep the same as ipv6 or ipv4
         * still recognize it as invalid ip if ip exceed the valid range
         */ 
        m_err_str = "invalid ip: " + std::string(ip_str);
        return false;
    }
    return true;
}

bool IPRange::mask_range(Range *range, unsigned short cidr)
{
    if (IPRANGE_IS_IPV4(range->from)) { /* ipv4 */
        if (cidr < 1 || cidr > 32) {
            m_err_str = "invalid cidr for ipv4: " + cidr;
            return false;
        }

        range->from.ip_32.a &= MASK_FROM_LUT[cidr];
        range->to.ip_32.a |= MASK_TO_LUT[cidr];
    } else { /* ipv6 */
        if (cidr < 1 || cidr > 128) {
            m_err_str = "invalid cidr for ipv6: " + cidr;
            return false;
        }
        unsigned short complement = cidr % 32; /* the result is less or equal to 31 */
     
        if (cidr > 96) {
            range->from.ip_32.a &= MASK_FROM_LUT[complement];
            range->to.ip_32.a |= MASK_TO_LUT[complement];
        } else if (cidr > 64) {
            range->from.ip_32.b &= MASK_FROM_LUT[complement];
            range->to.ip_32.b |= MASK_TO_LUT[complement];
            range->from.ip_32.a = 0;
            range->to.ip_32.a = 0xFFFFFFFF;
        } else if (cidr > 32) {
            range->from.ip_32.c &= MASK_FROM_LUT[complement];
            range->to.ip_32.c |= MASK_TO_LUT[complement];
            range->from.ip_32.b = 0;
            range->to.ip_32.b = 0xFFFFFFFF;
            range->from.ip_32.a = 0;
            range->to.ip_32.a = 0xFFFFFFFF;
        } else {
            range->from.ip_32.d &= MASK_FROM_LUT[complement];
            range->to.ip_32.d |= MASK_TO_LUT[complement];
            range->from.ip_32.c = 0;
            range->to.ip_32.c = 0xFFFFFFFF;
            range->from.ip_32.b = 0;
            range->to.ip_32.b = 0xFFFFFFFF;
            range->from.ip_32.a = 0;
            range->to.ip_32.a = 0xFFFFFFFF;
        }
    }
    return true;
}

/* 
 * parse the ip with mask into range sturst , format is as below:
 * x.x.x.x|x, ptr is the postion of "|"
 */
bool IPRange::parse_mask(const char* range, size_t range_len, const char *ptr, Range *new_range)
{
    if (range_len > 100) {
        m_err_str = "the range string length is not valid: " + range_len;
        return false;
    }

    char first_ip_str[IP_MAX_LEN] = {0};
    char mask_ip_str[IP_MAX_LEN] = {0};
    size_t first_ip_str_len = ptr - range;
    size_t mask_ip_str_len = range_len - 1 - first_ip_str_len;
    /* copy the first ip */
    copy_without_spaces(first_ip_str, sizeof(first_ip_str), range, first_ip_str_len);
    /* get the other ip */
    copy_without_spaces(mask_ip_str, sizeof(mask_ip_str), ptr + 1, mask_ip_str_len);
    IPV6 ip;
    IPV6 mask_ip;
    if (!str_to_ip(first_ip_str, &ip)) {
        m_err_str = "failed to convert ip: " + std::string(first_ip_str);
        return false;
    }
    if (!str_to_ip(mask_ip_str, &mask_ip)) {
        m_err_str = "failed to convert mask ip: " + std::string(mask_ip_str);
        return false;
    }
    if (mask_ip.ip_32.b == 0x0000FFFF) { /* ipv4 */
        new_range->from.ip_32.a = ip.ip_32.a & mask_ip.ip_32.a;
        new_range->from.ip_32.b = 0x0000FFFF;
        new_range->from.ip_64.upper = 0;
        new_range->to.ip_32.a = ip.ip_32.a | ~mask_ip.ip_32.a;
        new_range->to.ip_32.b = 0x0000FFFF;
        new_range->to.ip_64.upper = 0;
    } else {
        new_range->from = ip & mask_ip;
        new_range->to = ip | ~mask_ip;
    }
    return true;
}

bool IPRange::parse_single(const char* range, size_t range_len, Range *new_range)
{
    if (range_len > 100) {
        m_err_str = "the range string length is not valid: " + range_len;
        return false;
    }
    char buf[IP_MAX_LEN] = {0};
    /* copy the ip part to buf */
    copy_without_spaces(buf, sizeof(buf), range, range_len);
    if (!str_to_ip(buf, &(new_range->from))) {
        m_err_str = "failed to convert ip: " + std::string(buf);
        return false;
    }
    new_range->to = new_range->from;
    return true;
}

bool IPRange::parse_slash(const char* range, size_t range_len, const char *ptr, Range *new_range)
{   
    if (range_len > 100) {
        m_err_str = "the range string length is not valid: " + range_len;
        return false;
    }
    char buf[IP_MAX_LEN] = {0};
    size_t real_range_len = ptr - range;
    /* copy the ip part to buf */
    copy_without_spaces(buf, sizeof(buf), range, real_range_len);
    /* get the CIDR */
    unsigned short cidr = (unsigned short)atoi(ptr + 1);
    if (!str_to_ip(buf, &(new_range->from))) {
        m_err_str = "failed to convert ip: " + std::string(buf);
        return false;
    }
    new_range->to = new_range->from;
    (void)mask_range(new_range, cidr);
    return true;
}

bool IPRange::parse_hyphen(const char* range, size_t range_len, const char *ptr, Range *new_range)
{   
    if (range_len > 100) {
        m_err_str = "the range string length is not valid: " + range_len;
        return false;
    }
    /* clean white spaces */
    char first_ip[IP_MAX_LEN] = {0};
    char second_ip[IP_MAX_LEN] = {0};

    size_t first_ip_len = ptr - range;
    size_t second_ip_len = range_len - 1 - first_ip_len;
    /* copy the first ip */
    copy_without_spaces(first_ip, sizeof(first_ip), range, first_ip_len);
    /* get the other ip */
    copy_without_spaces(second_ip, sizeof(second_ip), ptr + 1, second_ip_len);
    if (!str_to_ip(first_ip, &(new_range->from))) {
        m_err_str = "failed to parse ip: " + std::string(first_ip);
        return false;
    }
    if (!str_to_ip(second_ip, &(new_range->to))) {
        m_err_str = "failed to parse ip: " + std::string(second_ip);
        return false;
    }
    if (new_range->from > new_range->to) {
        m_err_str =
            "the first ip (" + std::string(first_ip) + ") is bigger than the other (" + std::string(second_ip) + ")";
        return false;
    }
    return true;
}

void IPRange::handle_remove_intersection(Ranges_t *new_ranges, const Range *remove_range, Range *exist_range)
{
    IPV6 range_min = std::max(remove_range->from, exist_range->from);
    IPV6 range_max = std::min(remove_range->to, exist_range->to);
    if (range_min > range_max) {
        /* no intersaction */
        new_ranges->push_back(*exist_range);
        return;
    }
    /* there is an intersaction the remove_range includes the exist_range */
    if ((remove_range->from <= exist_range->from) && (remove_range->to >= exist_range->to)) {
        /* remove the exist range  */
        return;
    }
    // example:
    //      exist_range: 2 - 5
    //      remove_range: 1 - 3
    //      expected result: 4 - 5 
    if (remove_range->from <= exist_range->from) {
        exist_range->from = remove_range->to + 1;
        new_ranges->push_back(*exist_range);
        return;
    }
    if (remove_range->to >= exist_range->to) {
        exist_range->to = remove_range->from - 1;
        new_ranges->push_back(*exist_range);
        return;
    }
    /* the remove range is inside the exist one */
    new_ranges->push_back({exist_range->from, remove_range->from - 1});
    new_ranges->push_back({remove_range->to + 1, exist_range->to});
}

bool IPRange::handle_add_intersection(Range *new_range, const Range *exist_range)
{
    IPV6 range_min = std::max(new_range->from, exist_range->from);
    IPV6 range_max = std::min(new_range->to, exist_range->to);
    if (range_min > range_max) {
        return false;
    }

    new_range->from = std::min(new_range->from, exist_range->from);
    new_range->to = std::max(new_range->to, exist_range->to);
    return true;
}

/* 
 *  parse the ip range support below format
 *  single ip: 127.0.0.1
 *  range ip: 127.0.0.1 ~ 127.0.0.2
 *  cidr ip : 192.168.10.1/21
 *  mask ip : 192.168.10.1/255.255.255.2
 */
bool IPRange::parse_range(const char *range, size_t range_len, Range *new_range)
{
    /* handle format of "ip/cidr" */
    const char *ptr = (const char *)memchr(range, '/', range_len);
    if (ptr != NULL) {
        if (!parse_slash(range, range_len, ptr, new_range)) {
            m_err_str = "failed with parsing the range: " + std::string(range);
            return false;
        }
        return true;
    }
    /* handle format of "ip from-ip to" */
    ptr = (const char *)memchr(range, '-', range_len);
    if (ptr != NULL) {
        if (!parse_hyphen(range, range_len, ptr, new_range)) {
            m_err_str = "failed with parsing the range: " + std::string(range);
            return false;
        }
        return true;
    }
    /* handle format of "ip | ip mask" */
    ptr = (const char *)memchr(range, '|', range_len);
    if (ptr != NULL) {
        if (!parse_mask(range, range_len, ptr, new_range)) {
            m_err_str = "failed with parsing the range: " + std::string(range);
            return false;
        }
        return true;
    } else { /* handle single ip address as range */
        if (!parse_single(range, range_len, new_range)) {
            m_err_str = "failed with parsing the range: " + std::string(range);
            return false;
        }
        return true;
    }
    m_err_str = "unknown error parsing ip: " + std::string(range);
    return false;
}

bool IPRange::add_ranges(const std::unordered_set<std::string> ranges)
{
    for (const std::string range : ranges) {
        if (!add_range(range.c_str(), range.length())) {
            return false;
        }
    }
    return true;
}

bool IPRange::remove_ranges(const std::unordered_set<std::string> ranges)
{
    for (const std::string range : ranges) {
        if (!remove_range(range.c_str(), range.length())) {
            return false;
        }
    }
    return true;
}

bool IPRange::add_range(Range *new_range)
{
    /* adding the new range */
    if (m_ranges.size() == 0 || new_range->to < m_ranges[0].from) {
        (void)m_ranges.insert(m_ranges.begin(), *new_range);
        return true;
    } else if (new_range->from > m_ranges.back().to) {
        m_ranges.push_back(*new_range);
        return true;
    }
    Ranges_t new_ranges;
    bool we_had_intersection = false;
    uint32_t i = 0;
    /* interate over the ranges and check for intersection or the place to add the new range */
    while (i < m_ranges.size()) {
        /* in case of intersaction update the new range */
        if (handle_add_intersection(new_range, &m_ranges[i])) {
            we_had_intersection = true;
            ++i;
            continue;
        }
        /* just insert the intersaction */
        if (we_had_intersection) {
            we_had_intersection = false;
            new_ranges.push_back(*new_range);
            break;
        } else if (new_range->to < m_ranges[i].from) {
            /* we got the plcae to put the new range */
            new_ranges.push_back(*new_range);
            break;
        }
        /* just add the old range */
        new_ranges.push_back(m_ranges[i]);
        ++i;
    }
    /* if the intersection was until the end of the list - add it now */
    if (we_had_intersection) {
        new_ranges.push_back(*new_range);
    }
    /* copy the rest of the list if exist */
    while (i < m_ranges.size()) {
        new_ranges.push_back(m_ranges[i]);
        ++i;
    }
    m_ranges.swap(new_ranges);
    return true;
}

bool IPRange::is_range_valid(const std::string range)
{
    IPRange tmp;
    Range new_range;
    return tmp.parse_range(range.c_str(), range.size(), &new_range);
}

bool IPRange::add_range(const char* range, size_t range_len)
{
    Range new_range;
    m_err_str.clear();
    if (!parse_range(range, range_len, &new_range)) {
        return false;
    }
    return add_range(&new_range);
}

bool IPRange::remove_range(const char *range, size_t range_len)
{
    Ranges_t new_ranges;
    Range remove_range;
    m_err_str.clear();
    if (!parse_range(range, range_len, &remove_range)) {
        return false;
    }
    for (Range exist_range : m_ranges) {
        handle_remove_intersection(&new_ranges, &remove_range, &exist_range);
    }
    m_ranges.swap(new_ranges);
    return true;
}

std::string IPRange::ip_to_str(const IPV6 *ip) const
{
    char ip_str[INET6_ADDRSTRLEN];
    /* now get it back and print it */
    if (IPRANGE_IS_IPV4(*ip)) {
        uint32_t tmp = htonl(ip->ip_32.a);
        (void)inet_ntop(AF_INET, &tmp, ip_str, INET_ADDRSTRLEN);
    } else {
        IPV6 tmp_ip = *ip;
        tmp_ip.ip_32.a = htonl(ip->ip_32.d);
        tmp_ip.ip_32.b = htonl(ip->ip_32.c);
        tmp_ip.ip_32.c = htonl(ip->ip_32.b);
        tmp_ip.ip_32.d = htonl(ip->ip_32.a);
        (void)inet_ntop(AF_INET6, &tmp_ip, ip_str, INET6_ADDRSTRLEN);
    }
    return std::string(ip_str);
}

bool IPRange::binary_search(const IPV6 ip) const
{
    /* do a binary search */
    size_t i = 0;
    size_t j = m_ranges.size() - 1;
    size_t mid = 0;
    while (i != j) {
        mid = (i + j) / 2;
        if (ip >= m_ranges[mid].from && ip <= m_ranges[mid].to) {
            return true;
        }
        if (ip < m_ranges[mid].from) {
            j = (mid > 0) ? (mid - 1) : mid;
        } else {
            i = mid + 1;
        }
    }
    return (ip >= m_ranges[i].from && ip <= m_ranges[i].to);
}

bool IPRange::is_intersect(const IPRange *arg)
{
    for (size_t i = 0; i < m_ranges.size(); ++i) {
        Range tmp(m_ranges[i].from, m_ranges[i].to);
        for (size_t j = 0 ; j < arg->m_ranges.size(); ++j) {
            if (handle_add_intersection(&tmp, &arg->m_ranges[j])) {
                return true;
            }
        }
    }
    return false;
}

bool IPRange::is_in_range(const char *ip_str)
{
    IPV6 ip;
    if (!str_to_ip(ip_str, &ip)) {
        return false;
    }
    return is_in_range(&ip);
}

bool IPRange::is_in_range(const IPV6 *ip)
{
    if (m_ranges.size() == 0) {
        m_err_str = "there are no ranges in this object";
        return false;
    }
    m_err_str.clear();
    if (*ip == localhost_ipv4 || *ip == localhost_ipv6) {
        return binary_search(localhost_ipv4) || binary_search(localhost_ipv6);
    }
    return binary_search(*ip);
}

bool IPRange::is_in_range(const uint32_t ipv4)
{
    IPV6 ip;
    net_ipv4_to_host_order(&ip, (struct in_addr*)&ipv4);
    return is_in_range(&ip);
}

std::unordered_set<std::string> IPRange::get_ranges_set()
{
    std::unordered_set<std::string> rslt;
    for (Range range : m_ranges) {
        if (ip_to_str(&range.from).compare(ip_to_str(&range.to)) == 0) {
            rslt.insert(ip_to_str(&range.from));
        } else {
            rslt.insert(ip_to_str(&range.from) + "-" + ip_to_str(&range.to));
        }
    }
    return rslt;
}

void IPRange::copy_without_spaces(char buf[], size_t buf_len, const char *original, size_t original_len) const
{
    if (original_len == 0 || original_len > buf_len) {
        return;
    }
    char *p = buf;
    for (uint32_t i = 0; i < original_len; ++i) {
        if (original[i] != ' ') {
            *p++ = original[i];
        }
    }
    *p = '\0';
}

bool IPRange::empty() const
{
    return m_ranges.empty();
}

