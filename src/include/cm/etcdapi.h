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
 * etcdapi.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/etcdapi.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _ETCD_API_H_
#define _ETCD_API_H_

typedef int EtcdSession;

typedef enum {
    ETCD_OK = 0,
    ETCD_WTF = -1,
    EcodeKeyNotFound = 100,
    EcodeTestFailed = 101,
    EcodeNotFile = 102,
    ecodeNoMorePeer = 103,
    EcodeNotDir = 104,
    EcodeNodeExist = 105,
    ecodeKeyIsPreserved = 106,
    EcodeRootROnly = 107,
    EcodeDirNotEmpty = 108,
    ecodeExistingPeerAddr = 109,
    EcodeUnauthorized = 110,
    ecodeValueRequired = 200,
    EcodePrevValueRequired = 201,
    EcodeTTLNaN = 202,
    EcodeIndexNaN = 203,
    ecodeValueOrTTLRequired = 204,
    ecodeTimeoutNaN = 205,
    ecodeNameRequired = 206,
    ecodeIndexOrValueRequired = 207,
    ecodeIndexValueMutex = 208,
    EcodeInvalidField = 209,
    EcodeInvalidForm = 210,
    EcodeRefreshValue = 211,
    EcodeRefreshTTLRequired = 212,
    EcodeRaftInternal = 300,
    EcodeLeaderElect = 301,
    EcodeWatcherCleared = 400,
    EcodeEventIndexCleared = 401,
    ecodeStandbyInternal = 402,
    ecodeInvalidActiveSize = 403,
    ecodeInvalidRemoveDelay = 404,
    ecodeClientInternal = 500
} EtcdResult;

typedef struct {
    // PrevValue specifies what the current value of the Node must
    // be in order for the Set operation to succeed.
    // Leaving this field empty or NULL means that the caller wishes to
    // ignore the current value of the Node. This cannot be used
    // to compare the Node's current value to an empty string.
    // PrevValue is ignored if Dir=true
    char* prevValue;

    // PrevIndex indicates what the current ModifiedIndex of the
    // Node must be in order for the Set operation to succeed.
    //
    // If PrevIndex is set to 0 (default), no comparison is made.
    uint64_t prevIndex;

    // TTL defines a period of time after-which the Node should
    // expire and no longer exist. Values <= 0 are ignored. Given
    // that the zero-value is ignored, TTL cannot be used to set
    // a TTL of 0.
    int64_t ttl;

    // PrevExist specifies whether the Node must currently exist
    // (PrevExist) or not (PrevNoExist). If the caller does not
    // care about existence, set PrevExist to PrevIgnore, or simply
    // leave it unset.
    char prevExist[5];

    // Refresh set to true means a TTL value can be updated
    // without firing a watch or changing the node value. A
    // value must not be provided when refreshing a key.
    bool refresh;

    // Dir specifies whether or not this Node should be created as a directory.
    bool dir;

    // NoValueOnSuccess specifies whether the response contains the current value of the Node.
    // If set, the response will only contain the current value when the request fails.
    bool noValueOnSuccess;
} SetEtcdOption;

typedef struct {
    // Recursive defines whether or not all children of the Node
    // should be returned.
    bool recursive;

    // Sort instructs the server whether or not to sort the Nodes.
    // If true, the Nodes are sorted alphabetically by key in
    // ascending order (A to z). If false (default), the Nodes will
    // not be sorted and the ordering used should not be considered
    // predictable.
    bool sort;

    // Quorum specifies whether it gets the latest committed value that
    // has been applied in quorum of members, which ensures external
    // consistency (or linearizability).
    bool quorum;
} GetEtcdOption;

typedef struct {
    // PrevValue specifies what the current value of the Node must
    // be in order for the Delete operation to succeed.
    //
    // Leaving this field empty means that the caller wishes to
    // ignore the current value of the Node. This cannot be used
    // to compare the Node's current value to an empty string.
    char* prevValue;

    // PrevIndex indicates what the current ModifiedIndex of the
    // Node must be in order for the Delete operation to succeed.
    //
    // If PrevIndex is set to 0 (default), no comparison is made.
    uint64_t prevIndex;

    // Recursive defines whether or not all children of the Node
    // should be deleted. If set to true, all children of the Node
    // identified by the given key will be deleted. If left unset
    // or explicitly set to false, only a single Node will be
    // deleted.
    bool recursive;

    // Dir specifies whether or not this Node should be removed as a directory.
    bool dir;
} DeleteEtcdOption;

typedef struct {
    char* host;
    unsigned short port;
} EtcdServerSocket;

const int ETCD_MAX_PATH_LEN = 1024;

typedef struct {
    char etcd_ca_path[ETCD_MAX_PATH_LEN];
    char client_crt_path[ETCD_MAX_PATH_LEN];
    char client_key_path[ETCD_MAX_PATH_LEN];
} EtcdTlsAuthPath;

#define ETCD_VALUE_LEN 128
#define ERR_LEN 1024
#define ETCD_STATE_LEN 16
/* The default timeout is 2s */
const int ETCD_DEFAULT_TIMEOUT = 2000;

/*
 * etcd_open
 *
 * Establish a session to an etcd cluster.On success, 0 is returned;
 * on failure, an error code is returned
 *
 *      session
 *      The index of the session being created
 *
 *      server_list
 *      Array of etcd_server structures, with the last having host=NULL.  The
 *      caller is responsible for ensuring that this remains valid as long as
 *      the session exists.
 *
 *      tls_path
 *      Certificate information required for HTTPS connection
 *
 *      time_out
 *      This parameter is used to set the session timeout period.
 */
int etcd_open(EtcdSession* session, EtcdServerSocket* server_list, EtcdTlsAuthPath* tls_path, int time_out);

/*
 * etcd_close
 *
 * Terminate a session, closing connections and freeing memory (or any other
 * resources) associated with it.
 *
 *      session
 *      The index of the session to be closed.
 */
int etcd_close(int session);

/*
 * etcd_open_str
 *
 * Create a session for the ETCD cluster with a string containing
 * the list of services and certificates and timeout information.
 *
 *      session
 *      The index of the session being created
 *
 *      server_names
 *      String containing the service list. If the session information
 *      exists, The caller is responsible for ensuring that this remains
 *      valid as long as the session exists.
 *
 *      tls_path
 *      Certificate information required for HTTPS connection
 *
 *      timeout
 *      This parameter is used to set the session timeout period.
 */
int etcd_open_str(EtcdSession* session, char* server_names, EtcdTlsAuthPath* tls_path, int timeout);

/*
 * etcd_set
 *
 * Write a key, with optional previous value (as a precondition).
 *
 *     session
 *     The index of the session
 *
 *      key
 *      The etcd key (path) to set.
 *
 *      value
 *      New value as a null-terminated string.
 *
 *      option
 *      Indicates the operation information structure of the set operation.
 *      Currently, only prevValue is used. If prevValue = NULL, prevValue
 *      is not used. That is, the value of the current node is not considered.
 */
int etcd_set(EtcdSession session, char* key, char* value, SetEtcdOption* option);

/*
 * etcd_get
 *
 * Fetch a key from one of the servers in a session.On success, 0 is returned;
 * on failure, an error code is returned
 *
 *     session
 *     The index of the session
 *
 *      key
 *      The etcd key (path) to fetch.
 *
 *      value
 *      The value of the key, it is a newly allocated string, which must be
 *      freed by the caller.
 *
 *      max_size
 *      defines the max buffer size of health_member
 *
 *      option
 *      Operation information structure of the Get operation. Currently,
 *      only quorum is used to specify whether to obtain the latest committed
 *      value applied in the member arbitration to ensure external consistency
 *      (or linearization).
 */
int etcd_get(int session, char* key, char* value, int max_size, GetEtcdOption* option);

/*
 * etcd_delete
 *
 * Delete a key from one of the servers in a session.
 *
 *     session
 *     The index of the session
 *
 *      key
 *      The etcd key (path) to delete.
 *
 *      option
 *      Indicates the operation information structure of the Delete operation.
 *      No information is used at present.
 */
int etcd_delete(int session, char* key, DeleteEtcdOption* option);

/*
 * etcd_cluster_health
 *
 * Get the health of the cluster.If member_name is not empty, check the health
 * status of the corresponding etcd node and store the health status into
 * health_member. If member_name is empty, check the health status of the entire
 * etcd cluster and write health_member.
 *
 *     session
 *     The index of the session
 *
 *      member_name
 *      etcd member name,If this parameter is set to NULL or an empty string,
 *      the health status of the ETCD cluster is obtained. If the character
 *      string is a node name, it indicates the health status of a node.
 *
 *      health_state
 *      Save the etcd cluster health information
 *
 *      state_Size
 *      defines the max buffer size of health_member
 */
int etcd_cluster_health(int session, char* member_name, char* health_state, int state_Size);

/*
 * etcd_cluster_state
 *
 * Get the state of the cluster member.
 *
 *     session
 *     The index of the session
 *
 *      member_name
 *      etcd member name
 *
 *      is_leader
 *      Whether this node is the leader node or not
 */
int etcd_cluster_state(int session, char* member_name, bool* is_leader);

/*
 * get_last_error
 *
 * Obtains the information that fails to be invoked and saves the information to the buffer. If the interface is
 * successfully invoked, the buffer is empty.
 *
 */
const char* get_last_error();

#endif
