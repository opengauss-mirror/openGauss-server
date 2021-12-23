/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * comm_common.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_common.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <paths.h>
#include <errno.h>
#include <ctype.h>
#include <sstream>
#include <arpa/inet.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/optional/optional.hpp>
#include "comm_adapter.h"
#include "comm_core.h"
#include "comm_connection.h"
#include "communication/commproxy_interface.h"
#include "communication/libnet_extern.h"

#define DIE_STRING_LENGTH  5

#define COMM_MAX_IPADDR_LEN 32

static bool CommCheckFilterMatch(const char *filter, int len, const char *ip, int port);

void SetCPUAffinity(int cpu_id)
{
    cpu_set_t mask;
    cpu_set_t get;

    CPU_ZERO(&mask);
    CPU_SET(cpu_id, &mask);
    CPU_ZERO(&get);

    if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("set CPU affinity fail cpuid:%d.", cpu_id),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }

    if (sched_getaffinity(0, sizeof(get), &get) == -1) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("can not get CPU affinity cpuid:%d.", cpu_id),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }

    int num = sysconf(_SC_NPROCESSORS_CONF);

    for (int i = 0; i < num; i++) {
        if (CPU_ISSET(i, &get)) {
            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s CPU [%d]is bind OK total cpu:%d.",
                t_thrd.proxy_cxt.identifier, i, num)));
        } else {
            ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("%s CPU [%d]is bind FAIL total cpu:%d.",
                t_thrd.proxy_cxt.identifier, i, num)));
        }
    }
}

/*
 ******************************************************************************************
 ** Amtoic functions
 ******************************************************************************************
 **/
uint64 comm_atomic_add_fetch_u64(volatile uint64* ptr, uint32 inc)
{
#ifdef __aarch64__
    return __lse_atomic_fetch_add_u64(ptr, inc);
#else
    return __sync_fetch_and_add(ptr, inc) + inc;
#endif

}

uint32 comm_atomic_add_fetch_u32(volatile uint32* ptr, uint32 inc)
{
#ifdef __aarch64__
    return __lse_atomic_fetch_add_u32(ptr, inc);
#else
    return __sync_fetch_and_add(ptr, inc) + inc;
#endif

}

uint64 comm_atomic_fetch_sub_u64(volatile uint64* ptr, uint32 inc)
{
#ifdef __aarch64__
    return __lse_atomic_fetch_sub_u64(ptr, inc);
#else
    return __sync_fetch_and_sub(ptr, inc);
#endif
}

uint32 comm_atomic_fetch_sub_u32(volatile uint32* ptr, uint32 inc)
{
#ifdef __aarch64__
    return __lse_atomic_fetch_sub_u32(ptr, inc);
#else
    return __sync_fetch_and_sub(ptr, inc);
#endif
}

uint64 comm_atomic_read_u64(volatile uint64* ptr)
{
    return *ptr;
}

uint32 comm_atomic_read_u32(volatile uint32* ptr)
{
    return *ptr;
}

void comm_atomic_wirte_u32(volatile uint32* ptr, uint32 val)
{
    *ptr = val;
}

bool comm_compare_and_swap_32(volatile int32* dest, int32 oldval, int32 newval)
{
    if (oldval == newval)
        return true;

    volatile bool res = __sync_bool_compare_and_swap(dest, oldval, newval);

    return res;
}

/*
 ******************************************************************************************
 ** Libnet helper functions
 ******************************************************************************************
 **/
bool CommSetBlockMode(Sys_fcntl fn, int sock, bool is_block)
{
    int flg, rc;

    if ((flg = fn(sock, F_GETFL, 0)) < 0) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("fcntl get fd[%d] block mode failed! errno:%d, %m.",
            sock, errno)));
        return false;
    }

    if (is_block) {
        flg = flg & ~O_NONBLOCK;
    } else {
        flg |= O_NONBLOCK;
    }

    if ((rc = fn(sock, F_SETFL, flg)) < 0) {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("fcntl set fd[%d] %s failed! errno:%d, %m.",
            sock, is_block ? "BLOCK" : "O_NONBLOCK", errno)));
        return false;
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("set fd:[%d] %s success.",
        sock, is_block ? "BLOCK" : "O_NONBLOCK")));

    return true;
}


bool CommLibNetIsNoProxyLibosFd(int fd)
{
    if (fd < 0 || g_comm_controller == NULL) {
        return false;
    }

    if (g_comm_controller->FdGetCommSockDesc(fd) != NULL) {
        return false;
    }

#ifdef USE_LIBNET
    if (is_libos_fd(fd)) {
        return true;
    }
#endif

    return false;
}

bool CommLibNetIsLibosFd(phy_proto_type_t proto_type, int fd)
{
    if (fd < 0 || g_comm_controller == NULL) {
        return false;
    }

    if (proto_type == phy_proto_tcp) {
        return true;
    }

#ifdef USE_LIBNET
    if (proto_type == phy_proto_libnet && is_libos_fd(fd)) {
        return true;
    }
#endif

    return false;
}

int CommLibNetSetFdType(int fd, CommFdTypeOpt type)
{
#ifdef USE_LIBNET
    if (g_comm_proxy_config.s_enable_libnet) {
        return set_fd_type(fd, type);
    }
#endif

    return 0;
}

IPAddrType CommLibNetGetIPType(unsigned int ip)
{
    /** 127.0.0.1 */
    const unsigned int IPADDR_LOOPBACK    = ((unsigned int)0x7f000001UL);
    /** 0.0.0.0 */
    const unsigned int IPADDR_ANY         = ((unsigned int)0x00000000UL);
    /** 255.255.255.255 */
    const unsigned int IPADDR_BROADCAST   = ((unsigned int)0xffffffffUL);

#ifdef USE_LIBNET
    if (g_comm_proxy_config.s_enable_libnet) {
        return (IPAddrType)get_ip_type(ip);
    } else 
#endif
    {
        if (ip == IPADDR_ANY) {
            return IpAddrTypeAny;
        }

        if (ip == IPADDR_LOOPBACK) {
            return IpAddrTypeLoopBack;
        }

        if (ip == IPADDR_BROADCAST) {
            return IpAddrTypeBroadCast;
        }

        return IpAddrTypeOther;
    }
}

#define CMD_STR_MAX 512
#define CMD_OUTPUT_BUFFER_SIZE 1024

int CommCheckLtranProcess()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    char* cmd = NULL;
    char buf[CMD_OUTPUT_BUFFER_SIZE];
    int ltran_exist = 0;
    cmd = (char*)comm_malloc(CMD_STR_MAX * sizeof(char));
    if (cmd == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("malloc memory [%lu] failed, errno:%d,%m.", CMD_STR_MAX * sizeof(char), errno),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));

        return 0;
    }

    int rc = snprintf_s(cmd, CMD_STR_MAX, CMD_STR_MAX - 1, "ps ux | grep ltran | grep -v grep | grep config-file");
    if (rc < 0) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("printf check ltran cmd failed, errnno:%d, %m.", errno),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        comm_free(cmd);

        return 0;
    }

    FILE* fp = NULL;
    fp = popen(cmd, "r");
    ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("[cmdout] %s.", cmd)));
    if (fp == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("[error] exec cmd(%s) failed, errnno:%d, %m.", cmd, errno),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        return 0;
    } else {
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            ltran_exist = 1;
            ereport(DEBUG2, (errmodule(MOD_COMM_PROXY), errmsg("[cmdout] %s.", buf)));
        }
        pclose(fp);
    }
    comm_free(cmd);

    return ltran_exist;
}

static int FindCommProxyAttrKey(const char* str_attr, const char* key)
{
    int npos = -1;
    int attr_len = strlen(str_attr);
    int key_len = strlen(key); 
    for (int i = 0; i <= attr_len - key_len; ++i) {
        if (strncmp(&str_attr[i], key, key_len) == 0) {
            npos = i;
            break;
        }
    }

    return npos;
}

static void ReportParseCommProxyError(const char* key)
{
    ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("parse %s error in comm_proxy_attr.", key),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
}

template<typename T>
static T GetCommProxySubParameter(const char* str_attr, const char* key)
{
    T res = T();
    int attr_pos = FindCommProxyAttrKey(str_attr, key);
    if (attr_pos == -1) {
        ReportParseCommProxyError(key);
        exit(-1);
    }

    if (strcmp(key, "enable_libnet") == 0) {
        if (str_attr[attr_pos + strlen("enable_libnet:")] == 't') {
            res = true;
        } else {
            res = false;
        }
    } else if (strcmp(key, "enable_dfx") == 0) {
        if (str_attr[attr_pos + strlen("enable_dfx:")] == 't') {
            res = true;
        } else {
            res = false;
        }
    } else if (strcmp(key, "numa_num") == 0) {
        if (str_attr[attr_pos + strlen("numa_num:")] == '4') {
            res = 4;
        } else if (str_attr[attr_pos + strlen("numa_num:")] == '8') {
            res = 8;
        } else {
            ReportParseCommProxyError(key);
        }
    } else if (strcmp(key, "numa_bind") == 0) {
        res = attr_pos;
    }

    return res;
}

static void ParseCommProxyNumaBind(
    const char* str_attr, const int pos, const int numa_num, int* numa_bind)
{
    int numa_num_cnt = 0;

    for (int i = pos; str_attr[i] != '}'; ++i) {
        if (numa_num_cnt >= numa_num * CPU_NUM_PER_NUMA) {
            break;
        }
    
        if (str_attr[i] < '0' || str_attr[i] > '9') {
            continue;
        }

        int cpu_idx = 0;
        while (str_attr[i] >= '0' && str_attr[i] <= '9') {
            cpu_idx = cpu_idx * 10;
            cpu_idx += str_attr[i] - '0';
            ++i;
        }

        numa_bind[numa_num_cnt] = cpu_idx;
        ++numa_num_cnt;
    }
}

/*
 ******************************************************************************************
 ** Proxy attribute parsing functions
 ******************************************************************************************
 **/
bool ParseCommProxyAttr(CommProxyConfig* config)
{
    if (g_instance.attr.attr_common.comm_proxy_attr == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("the parameter comm_proxy_attr is NULL."),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
        return false;
    }

    char* attr = TrimStr(g_instance.attr.attr_common.comm_proxy_attr);
    int numa_bind[THREADPOOL_TOTAL_GROUP*CPU_NUM_PER_NUMA] = { 0 };
    int numa_bind_pos = 0;
    int all_comms = 0;
    int rc = 0;
    int proxy_group_id = 0;
    int listen_id = 0;
    char str_port[MAX_FILTER_STR_LEN] = "\0";

    rc = memset_s(config, sizeof(CommProxyConfig), '\0', sizeof(CommProxyConfig));
    securec_check(rc, "\0", "\0");
    rc = memset_s(numa_bind, sizeof(numa_bind), '\0', sizeof(numa_bind));
    securec_check(rc, "\0", "\0");
    
    rc = memset_s(str_port, sizeof(str_port), 0, sizeof(str_port));
    securec_check(rc, "\0", "\0");
    rc = sprintf_s(str_port, sizeof(str_port), "%d", g_instance.attr.attr_network.PostPortNumber);
    securec_check_ss(rc, "", "");

    rc = strncpy_s(config->s_comm_proxy_groups[proxy_group_id].s_listen[listen_id], MAX_FILTER_STR_LEN,
            g_instance.attr.attr_network.tcp_link_addr, strlen(g_instance.attr.attr_network.tcp_link_addr));
    securec_check_c(rc, "", "");
    rc = strcat_s(config->s_comm_proxy_groups[proxy_group_id].s_listen[listen_id], MAX_FILTER_STR_LEN, ":");
    securec_check(rc, "", "");
    rc = strcat_s(config->s_comm_proxy_groups[proxy_group_id].s_listen[listen_id], MAX_FILTER_STR_LEN, str_port);
    securec_check(rc, "", "");


    config->s_enable_libnet = GetCommProxySubParameter<bool>(attr, "enable_libnet");
    config->s_enable_dfx = GetCommProxySubParameter<bool>(attr, "enable_dfx");
    config->s_numa_num = GetCommProxySubParameter<int>(attr, "numa_num");
    config->s_comm_proxy_groups[proxy_group_id].s_comm_type = phy_proto_libnet;
    config->s_comm_proxy_group_cnt = 1;
    config->s_comm_proxy_groups[proxy_group_id].s_listen_cnt = 1;

    numa_bind_pos = GetCommProxySubParameter<int>(attr, "numa_bind");
    ParseCommProxyNumaBind(attr, numa_bind_pos, config->s_numa_num, numa_bind);
    for (int node_id = 0; node_id < config->s_numa_num; ++node_id) {
        for (int core_id = 0; core_id < CPU_NUM_PER_NUMA; ++core_id) {
            int cpu_idx = numa_bind[node_id*CPU_NUM_PER_NUMA+core_id];
            if ((config->s_numa_num == 4 && core_id > 127) || (config->s_numa_num == 8 && core_id > 255)) {
                ReportParseCommProxyError("numa_bind");
                return false;
            }

            config->s_comm_proxy_groups[proxy_group_id].s_comms[node_id][core_id] = cpu_idx;
            all_comms++;
        }
        for (int core_id = CPU_NUM_PER_NUMA; core_id < MAX_PROXY_CORE_NUM; core_id++) {
            config->s_comm_proxy_groups[proxy_group_id].s_comms[node_id][core_id] = -1;
        }
    }
    config->s_comm_proxy_groups[proxy_group_id].s_comm_nums = all_comms;

    return true;
}

static bool CommCheckFilterMatch(const char *filter, int len, const char *ip, int port)
{
    char *str_ip = NULL;
    char *str_port = NULL;
    char *endptr = NULL;
    int filter_port = 0;
    char ip_port[MAX_IP_POR_STR_LEN] = "\0";
    ip_port[MAX_IP_POR_STR_LEN-1] = '\0';
    int rc = memcpy_s(ip_port, sizeof(ip_port) - 1, filter, len);
    securec_check(rc, "\0", "\0");
    
    str_ip = strtok(ip_port, ":");
    str_port = strtok(NULL, ":");
    if (str_ip == NULL || str_port == NULL) {
        return false;
    }
    
    filter_port = strtol(str_port, &endptr, 10);
    if (*endptr == '\0' && strcmp(ip, str_ip) == 0 && port == filter_port) {
        return true;
    }

    return false;
}

/*
 * - brief set the IP/Port to be started in proxy mode, normally we invoke this function
 *         before comm_socket() as we pass-in ip/port thus make the proxy/bare-tcp protocol
 *         specification
 * input: option->conn_role
 * output: option->conn_attr, option->numa_map, option->group_id
 */
bool CommConfigSocketOption(CommConnSocketRole role, const char *ip, int port)
{
    /* do nothing when proxy is not specified */
    if (ip == NULL || g_comm_controller == NULL) {
        return false;
    }

    bool ret;
    for (int i = 0; i < g_comm_proxy_config.s_comm_proxy_group_cnt; i++) {
        if (role == CommConnSocketServer) {
            for (int j = 0; j < g_comm_proxy_config.s_comm_proxy_groups[i].s_listen_cnt; j++) {
                ret = CommCheckFilterMatch(
                    g_comm_proxy_config.s_comm_proxy_groups[i].s_listen[j],
                    sizeof(g_comm_proxy_config.s_comm_proxy_groups[i].s_listen[j]),
                    ip,
                    port);
                if (ret) {
                    CommSetConnOption(
                        role,
                        SetCommConnAttr(CommConnAttrTypeProxy, g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type),
                        g_comm_proxy_config.s_comm_proxy_groups[i].s_gen_numa_map,
                        i);

                    return true;
                }
            }
        } else if (role == CommConnSockerClient) {
            for (int j = 0; j < g_comm_proxy_config.s_comm_proxy_groups[i].s_connect_cnt; j++) {
                ret = CommCheckFilterMatch(
                    g_comm_proxy_config.s_comm_proxy_groups[i].s_connect[j],
                    sizeof(g_comm_proxy_config.s_comm_proxy_groups[i].s_connect[j]),
                    ip,
                    port);
                if (ret) {
                    CommSetConnOption(
                        role,
                        SetCommConnAttr(CommConnAttrTypeProxy, g_comm_proxy_config.s_comm_proxy_groups[i].s_comm_type),
                        g_comm_proxy_config.s_comm_proxy_groups[i].s_gen_numa_map,
                        i);

                    return true;
                }
            }
        }
    }

    for (int i = 0; i < g_comm_proxy_config.s_comm_pure_group_cnt; i++) {
        if (role == CommConnSocketServer) {
            for (int j = 0; j < g_comm_proxy_config.s_comm_pure_groups[i].s_listen_cnt; j++) {
                ret = CommCheckFilterMatch(
                    g_comm_proxy_config.s_comm_pure_groups[i].s_listen[j],
                    sizeof(g_comm_proxy_config.s_comm_pure_groups[i].s_listen[j]),
                    ip,
                    port);
                if (ret) {
                    CommSetConnOption(
                        role,
                        SetCommConnAttr(CommConnAttrTypePure, g_comm_proxy_config.s_comm_pure_groups[i].s_comm_type),
                        g_comm_proxy_config.s_comm_pure_groups[i].s_gen_numa_map,
                        i);

                    return true;
                }
            }
        } else if (role == CommConnSockerClient) {
            for (int j = 0; j < g_comm_proxy_config.s_comm_pure_groups[i].s_connect_cnt; j++) {
                ret = CommCheckFilterMatch(
                    g_comm_proxy_config.s_comm_pure_groups[i].s_connect[j],
                    sizeof(g_comm_proxy_config.s_comm_pure_groups[i].s_connect[j]),
                    ip,
                    port);
                if (ret) {
                    CommSetConnOption(
                        role,
                        SetCommConnAttr(CommConnAttrTypePure, g_comm_proxy_config.s_comm_pure_groups[i].s_comm_type),
                        g_comm_proxy_config.s_comm_pure_groups[i].s_gen_numa_map,
                        i);

                    return true;
                }
            }
        }
    }

    return false;
}

/*
 ******************************************************************************************
 ** Packet functions for packet create/drop/send
 ******************************************************************************************
 **/
Packet* CreatePacket(int socketfd, const char* sendbuff, char* recvbuff, int length, PacketType type)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    Packet* p = (Packet*)comm_malloc(sizeof(struct Packet));
    p->exp_size = length;
    p->cur_size = 0;
    p->comm_errno = 0;

    p->sockfd = socketfd;
    p->done = false;
    p->type = type;
    p->next_pkt = NULL;
    p->cur_off = 0;

    /* create regular package */
    if (type == PKT_TYPE_RECV) {
        p->data = (char *)comm_malloc(sizeof(char) * length);
        int rc = memset_s(p->data, length, 0, length);
        securec_check(rc, "\0", "\0");
        p->outbuff = NULL;
    } else {
        p->outbuff = NULL;
        p->data = NULL;
    }

    return p;
}

void DropPacket(Packet** pack)
{
    if (pack == NULL || *pack == NULL) {
        return;
    }

    if ((*pack)->type == PKT_TYPE_SEND) {
        comm_free((*pack)->outbuff);
    }
    comm_free_ext(*pack);
}

void ClearPacket(Packet** pack)
{
    if (pack == NULL || *pack == NULL) {
        return;
    }

    (*pack)->cur_size = 0;
    (*pack)->cur_off = 0;
    (*pack)->done = false;
    (*pack)->next_pkt = NULL;
    (*pack)->sockfd = -1;
    (*pack)->comm_errno = 0;

    if ((*pack)->type == PKT_TYPE_RECV) {
        /* expsize is fix value for packet object */
        int rc = memset_s((*pack)->data, (*pack)->exp_size, 0, (*pack)->exp_size);
        securec_check(rc, "\0", "\0");
    } else if ((*pack)->type == PKT_TYPE_SEND) {
        /* next user free buff */
        (*pack)->exp_size = 0;
    }
}


bool EndPacket(Packet* pack)
{

    if (pack == NULL) {
        return false;
    }

    if (pack->cur_size > 0 && pack->done) {

        return true;
    }

    if (pack->comm_errno != 0 && pack->comm_errno != EINTR && pack->comm_errno != EAGAIN) {
        pack->cur_size = -1;
        return true;
    }
    if (pack->cur_size > pack->exp_size) {
        Assert(0);
    }

    return false;
}

bool CheckPacketReady(Packet* pack)
{
    if (pack == NULL || pack->done == true) {
    /*
     ** pack == null, worker is not ready to recv
     ** pack->done == true, last packet is not be processed
     ** we can enter this at next epoll wait by lt
     **/
        return false;
    }

    if (pack->type == PKT_TYPE_RECV && pack->data == NULL) {
        /* this case maybe happen when cache not sync to memory */
        gaussdb_memory_barrier();
        return false;
    }

    if (pack->type == PKT_TYPE_SEND && pack->outbuff == NULL) {
        gaussdb_memory_barrier();
        return false;
    }

    return true;
}
