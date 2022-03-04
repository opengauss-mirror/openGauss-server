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
 * -------------------------------------------------------------------------
 *
 * iprange.h
 *    define operation functions for ip type data
 *
 * IDENTIFICATION
 *    src/include/iprange/iprange.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef IPRANGE_AUDIT_H_
#define IPRANGE_AUDIT_H_

#include <stdint.h>
#include <string>
#include <unordered_set>
#include <vector>

typedef unsigned char ipv6arr[16];

typedef struct IPV6_64_s
{
    uint64_t lower;
    uint64_t upper;
} IPV6_64;

typedef struct IPV6_32_s
{
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint32_t d;
} IPV6_32;

typedef union IPV6_s
{
    IPV6_64 ip_64;
    IPV6_32 ip_32;
} IPV6;

inline bool operator< (const IPV6 lhs, const IPV6 rhs){ return lhs.ip_64.upper < rhs.ip_64.upper || lhs.ip_64.lower < rhs.ip_64.lower; }
inline bool operator> (const IPV6 lhs, const IPV6 rhs){ return rhs < lhs; }
inline bool operator<=(const IPV6 lhs, const IPV6 rhs){ return !(lhs > rhs); }
inline bool operator>=(const IPV6 lhs, const IPV6 rhs){ return !(lhs < rhs); }

inline const IPV6 operator~(const IPV6 ip)
{
    IPV6 new_ip; 
    new_ip.ip_64.lower = ~ip.ip_64.lower;
    new_ip.ip_64.upper = ~ip.ip_64.upper;
    return new_ip;
}

inline bool operator==(const IPV6 lhs, const IPV6 rhs)
{
    return lhs.ip_64.lower == rhs.ip_64.lower && lhs.ip_64.upper == rhs.ip_64.upper;
}

inline const IPV6 operator+(const IPV6 lhs, const IPV6 rhs)
{
    uint64_t tmp;
    IPV6 new_ip {0,0};
    
    if (lhs.ip_32.b == 0x0000FFFF) {
        new_ip.ip_32.a = lhs.ip_32.a + rhs.ip_32.a;
        new_ip.ip_32.b = 0x0000FFFF;
        return new_ip;
    }
    
    new_ip.ip_32.a = tmp = ((int64_t)lhs.ip_32.a) + rhs.ip_32.a;
    new_ip.ip_32.b = tmp = ((int64_t)lhs.ip_32.b) + rhs.ip_32.b + (tmp >> 32);
    new_ip.ip_32.c = tmp = ((int64_t)lhs.ip_32.c) + rhs.ip_32.c + (tmp >> 32);
    new_ip.ip_32.d =       ((int64_t)lhs.ip_32.d) + rhs.ip_32.d + (tmp >> 32);
    return new_ip;
}

inline const IPV6 operator+(const IPV6 lhs, const int i)
{
    IPV6 new_ip {0,0};
    new_ip.ip_64.lower = i;
    return lhs + new_ip;
}

inline const IPV6 operator-(const IPV6 lhs, const IPV6 rhs)
{
    if (lhs.ip_32.b == 0x0000FFFF) {
        IPV6 new_ip {0,0};
        new_ip.ip_32.a = lhs.ip_32.a - rhs.ip_32.a;
        new_ip.ip_32.b = 0x0000FFFF;
        return new_ip;
    }
    
    uint64_t tmp;
    IPV6 new_ip {0,0};
    new_ip.ip_32.a = tmp = ((int64_t)lhs.ip_32.a) - rhs.ip_32.a;
    new_ip.ip_32.b = tmp = ((int64_t)lhs.ip_32.b) - rhs.ip_32.b - (tmp>>63);
    new_ip.ip_32.c = tmp = ((int64_t)lhs.ip_32.c) - rhs.ip_32.c - (tmp>>63);
    new_ip.ip_32.d =       ((int64_t)lhs.ip_32.d) - rhs.ip_32.d - (tmp>>63);
    return new_ip;
}

inline const IPV6 operator-(const IPV6 lhs, const int i)
{
    IPV6 new_ip {0,0};
    new_ip.ip_64.lower = i;
    return lhs - new_ip;
}
  
inline const IPV6 operator&(const IPV6 lhs, const IPV6 rhs)
{
    IPV6 new_ip;
    new_ip.ip_64.lower = lhs.ip_64.lower & rhs.ip_64.lower;
    new_ip.ip_64.upper = lhs.ip_64.upper & rhs.ip_64.upper;
    return new_ip;
}

inline const IPV6 operator|(const IPV6 lhs, const IPV6 rhs)
{
    IPV6 new_ip;
    new_ip.ip_64.lower = lhs.ip_64.lower | rhs.ip_64.lower;
    new_ip.ip_64.upper = lhs.ip_64.upper | rhs.ip_64.upper;
    return new_ip;
}

typedef struct Range_s
{
    Range_s(const IPV6 _from = IPV6(), const IPV6 _to = IPV6()):from(_from), to(_to){}
    IPV6 from;
    IPV6 to;
} Range;

class IPRange
{
public:

    IPRange();
    ~IPRange();
    
    static bool is_range_valid(const std::string range);
    bool add_ranges(const std::unordered_set<std::string> ranges);
    bool add_range(Range *new_range);
    bool add_range(const char *range, size_t range_len);
    bool remove_ranges(const std::unordered_set<std::string> ranges);
    bool remove_range(const char *range, size_t range_len);
    std::unordered_set<std::string> get_ranges_set();

    bool is_in_range(const char *ip_str);
    bool is_in_range(const IPV6 *ip);
    bool is_in_range(const uint32_t ipv4);
    bool is_intersect(const IPRange *arg);
    bool empty() const;
    
    const std::string& get_err_str() { return m_err_str; }
    std::string ip_to_str(const IPV6 *ip) const;
    bool str_to_ip(const char* ip_str, IPV6 *ip);
private:

    typedef std::vector<Range> Ranges_t;
    Ranges_t m_ranges;
    std::string m_err_str;

    bool parse_range(const char* range, size_t range_len, Range *new_range);
    bool parse_slash(const char* range, size_t range_len, const char *ptr, Range *new_range);
    bool parse_hyphen(const char* range, size_t range_len, const char *ptr, Range *new_range);
    bool parse_mask(const char* range, size_t range_len, const char *ptr, Range *new_range);
    bool parse_single(const char* range, size_t range_len, Range *new_range);
    bool binary_search(const IPV6 ip) const;

    bool mask_range(Range *range, unsigned short cidr);
    void handle_remove_intersection(Ranges_t *new_ranges, const Range *remove_range, Range *exist_range);
    bool handle_add_intersection(Range *new_range, const Range *exist_range);
    void copy_without_spaces(char buf[], size_t buf_len, const char *original, size_t original_len) const;
    void net_ipv6_to_host_order(IPV6 *ip, const struct sockaddr_in6 *sa) const;
    void net_ipv4_to_host_order(IPV6 *ip, const struct in_addr *addr) const;
};

#endif // IPRANGE_AUDIT_H_
