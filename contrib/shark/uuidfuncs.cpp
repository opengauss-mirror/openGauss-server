/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * --------------------------------------------------------------------------------------
 *
 * uuidfuncs.cpp
 *
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/shark/uuidfuncs.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/inet.h"
#include <net/if.h>

#define UUID_VERSION 0x1000
#define FORMATTED_UUID_LEN 36
#define UUID_FIRST_PART_LEN 8
#define UUID_MID_PART_LEN 4
#define UUID_LAST_PART_LEN 12
#define UUID_TIME_OFFSET ((uint64)141427 * 24 * 60 * 60 * 1000 * 1000 * 10)
#define INT_RANGE_REVISE_PARAM 128
#define HEX_BASE 16
#define URANDOM_FILE_PATH "/dev/urandom"
#define MAC_CHAR_NUM 6
#define CLOCK_SEQ_CHAR_NUM 2
#define MaxMacAddrList 10
#define HEX_CHARS "0123456789abcdef"

static uint64 uuid_time = 0;
static uint nano_seq = 0;
static char g_clockSeqAndMac[]="0000-000000000000";
static pthread_mutex_t gUuidMutex = PTHREAD_MUTEX_INITIALIZER;

PG_FUNCTION_INFO_V1(uuid_generate);
extern "C" Datum uuid_generate(PG_FUNCTION_ARGS);

/*
* transfer char* into hexadecimal style.
* offset is used to modify the range of source char in case of getting range of signed int8. set to 0 if not needed.
*/
static void string_to_hex(char* source, char* dest, int src_len, int offset)
{
    int dest_index = 0;
    for (int i = 0; i < src_len; i++) {
        dest[dest_index++] = HEX_CHARS[(source[i] + offset) / HEX_BASE];
        dest[dest_index++] = HEX_CHARS[(source[i] + offset) % HEX_BASE];
    }
}

static void int_to_hex(uint64 source, char* dest, int src_len)
{
    char* dest_ptr = dest + src_len;
    for (int i = 0; i < src_len; i++) {
        *(--dest_ptr) = HEX_CHARS[source & 0xF];
        source >>= 4;
    }
}

/*
 * get specific num of character read from /dev/urandom, storing in rand_buf.
 */
static void pseudo_rand_read(char* rand_buf, int bytes_read)
{
    FILE* f = fopen(URANDOM_FILE_PATH, "r");
    if (!f) {
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), (errmsg("cannot open urandom file"))));
    }
    size_t b_read;
    char* buf_ptr = rand_buf;
    while (bytes_read) {
        b_read = fread(buf_ptr, 1, bytes_read, f);
        if (b_read <= 0) {
            fclose(f);
            ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), (errmsg("failed to get random number"))));
        }
        buf_ptr += b_read;
        bytes_read -= (int)b_read;
    }
    fclose(f);
}

static uint64 GetMACAddr(void)
{
    macaddr mac;
    uint64 macAddr;
    int sockFd = NO_SOCKET;
    struct ifconf ifconfInfo;
    struct ifreq ifreqInfo;
    char *buf = NULL;
    errno_t ss_rc = EOK;
    uint32 i;

    ss_rc = memset_s((void *)&mac, sizeof(macaddr), 0, sizeof(macaddr));
    securec_check(ss_rc, "\0", "\0");

    sockFd = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sockFd != NO_SOCKET) {
        buf = (char *)palloc(MaxMacAddrList * sizeof(ifreq));
        ifconfInfo.ifc_len = MaxMacAddrList * sizeof(ifreq);
        ifconfInfo.ifc_buf = buf;

        if (ioctl(sockFd, SIOCGIFCONF, &ifconfInfo) != -1) {
            struct ifreq *ifrepTmp = ifconfInfo.ifc_req;
            for (i = 0; i < (ifconfInfo.ifc_len / sizeof(struct ifreq)); i++) {
                ss_rc = strcpy_s(ifreqInfo.ifr_name, strlen(ifrepTmp->ifr_name) + 1, ifrepTmp->ifr_name);
                securec_check(ss_rc, "\0", "\0");

                if (ioctl(sockFd, SIOCGIFFLAGS, &ifreqInfo) == 0) {
                    if (!(ifreqInfo.ifr_flags & IFF_LOOPBACK)) {
                        if (ioctl(sockFd, SIOCGIFHWADDR, &ifreqInfo) == 0) {
                            mac.a = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[0];
                            mac.b = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[1];
                            mac.c = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[2];
                            mac.d = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[3];
                            mac.e = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[4];
                            mac.f = (unsigned char)ifreqInfo.ifr_hwaddr.sa_data[5];
                            break;
                        }
                    }
                }
                ifrepTmp++;
            }
        }
        pfree_ext(buf);
        close(sockFd);
    }
    macAddr = ((uint64)mac.a << 40) | ((uint64)mac.b << 32) | ((uint64)mac.c << 24) | ((uint64)mac.d << 16) |
            ((uint64)mac.e << 8) | (uint64)mac.f;
    return macAddr;
}

/*
 * generate uuid in v1 style, same as uuid() function in dolphin,
 * difference in return type, dolphin is varchar, shark is uuid.
 */
Datum uuid_generate(PG_FUNCTION_ARGS)
{
    char clockSeq[CLOCK_SEQ_CHAR_NUM];
    /* two extra bytes, one for '-', one for '\0' */
    char clockSeqAndMac[UUID_MID_PART_LEN + UUID_LAST_PART_LEN + 2];
    AutoMutexLock localeLock(&gUuidMutex);
    localeLock.lock();

    // first time in this function. get mac address and clock seq first
    if (unlikely(!uuid_time)) {
        pseudo_rand_read(clockSeq, CLOCK_SEQ_CHAR_NUM);
        string_to_hex(clockSeq, g_clockSeqAndMac, CLOCK_SEQ_CHAR_NUM, INT_RANGE_REVISE_PARAM);

        uint64 mac_addr = GetMACAddr();
        // no mac obtained. init or use random string as virtual mac
        if (!mac_addr) {
            char virtual_mac_string[6] = {0};
            if (strlen(virtual_mac_string) == 0) {
                pseudo_rand_read(virtual_mac_string, MAC_CHAR_NUM);
            }
            mac_addr = ((uint64)virtual_mac_string[0] << 40) | ((uint64)virtual_mac_string[1] << 32) |
                    ((uint64)virtual_mac_string[2] << 24) | ((uint64)virtual_mac_string[3] << 16) |
                    ((uint64)virtual_mac_string[4] << 8) | (uint64)virtual_mac_string[5];
        }
        int_to_hex(mac_addr, g_clockSeqAndMac + UUID_MID_PART_LEN + 1, UUID_LAST_PART_LEN);
    }

    uint64 curr_time;
    /*
     * this is why nano_seq should be uint but not uint64, cause nano_seq will increase, if it is uint64
     * add increase to UINT64_MAX, the final result of curr_time will be overflow.
     */
#ifdef HAVE_INT64_TIMESTAMP
    curr_time = GetCurrentTimestamp() * 10 + UUID_TIME_OFFSET + nano_seq;
#else
    curr_time = GetCurrentTimestamp() * 1000 * 1000 * 10 + UUID_TIME_OFFSET + nano_seq;
#endif
    // give back nano_seq we borrowed before
    if (curr_time > uuid_time && nano_seq > 0) {
        uint64 give_back = Min(nano_seq, curr_time - uuid_time - 1);
        curr_time -= give_back;
        nano_seq -= give_back;
    }

    // borrow an extra nano_seq if curr_time == uuid_time to prevent timestamp collision
    if (curr_time == uuid_time) {
        /*
         * If nanoseq overflows, we need to start over with a new numberspace, cause in the next loop,
         * the value of curr_time may collide with an already generated value. So if nano_seq overflows,
         * we won't increase curr_time, then the curr_time will equal to uuid_time, which will lead to
         * new numberspace.
         */
        if (likely(++nano_seq)) {
            ++curr_time;
        }
    }

    // if first time creating uuid, or sys time changed backward, we reset clock_seq to prevent time stamp duplication.
    // meanwhile, we reset nano_seq.
    if (unlikely(curr_time <= uuid_time)) {
        pseudo_rand_read(clockSeq, CLOCK_SEQ_CHAR_NUM);
        string_to_hex(clockSeq, g_clockSeqAndMac, CLOCK_SEQ_CHAR_NUM, INT_RANGE_REVISE_PARAM);
        nano_seq = 0;
    }
    /* we need a local copy during hold the lock, so it won't be changed by other thread */
    int rc = strcpy_s(clockSeqAndMac, UUID_MID_PART_LEN + UUID_LAST_PART_LEN + 2, g_clockSeqAndMac);
    securec_check(rc, "", "");
    uuid_time = curr_time;
    localeLock.unLock();

    uint32 timestamp_low = (uint32)(curr_time & 0xFFFFFFFF);
    uint16 timestamp_mid = (uint16)((curr_time >> 32) & 0xFFFF);
    uint16 timestamp_high_and_v = (uint16)((curr_time >> 48) | UUID_VERSION);

    // result len equals to 32 + 4 * '-' + '\0'
    char* res = (char*)palloc(FORMATTED_UUID_LEN + 1);
    res[FORMATTED_UUID_LEN] = '\0';
    int cursor = 0;

    int_to_hex(timestamp_low, res, UUID_FIRST_PART_LEN);
    cursor += UUID_FIRST_PART_LEN;
    res[cursor++] = '-';

    int_to_hex(timestamp_mid, res + cursor, UUID_MID_PART_LEN);
    cursor += UUID_MID_PART_LEN;
    res[cursor++] = '-';

    int_to_hex(timestamp_high_and_v, res + cursor, UUID_MID_PART_LEN);
    cursor += UUID_MID_PART_LEN;
    res[cursor++] = '-';

    rc = strcpy_s(res + cursor, FORMATTED_UUID_LEN + 1 - cursor, clockSeqAndMac);
    securec_check(rc, "", "");

    return DirectFunctionCall1(uuid_in, CStringGetDatum(res));
}
