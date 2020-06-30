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
 * sctp_errno-comlib.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_errno-comlib.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _MC_ERRNO_COMLIB_H_
#define _MC_ERRNO_COMLIB_H_

static const char* comlib_error[] = {
    "1000   Reserved",
    "1001   Invalid argument",
    "1002   Memeory allocate error",
    "1003   Condition variable initialize error",
    "1004   Condition variable destroy error",
    "1005   Mutex locker initialize error",
    "1006   Mutex locker destroy error",
    "1007   Cannot get node index by socket or port",
    "1008   Cannot get node index by socket or port",
    "1009   Cannot set usable stream index of given node index",
    "1010   Abnormal buffer queue size",
    "1011   Abnormal quota value",
    "1012   Semaphore initialize error",
    "1013   Cannot post semaphore",
    "1014   Cannot wait semaphore",
    "1015   EPoller initialize error",
    "1016   Cannot get epoll handler",
    "1017   address initialize error",
    "1018   listen error",
    "1019   Consumer mailbox initialize error",
    "1020   Cannot get node index by port",
    "1021   Cannot get stream index, maybe comm_max_stream is not enough.",
    "1022   Invalid tcp socket",
    "1023   EPoller event error",
    "1024   Wrong control message",
    "1025   Control message write error",
    "1026   Control message read error",
    "1027   Invalid stream index",
    "1028   Invaild node index",
    "1029   Unexpected control message size",
    "1030   Condition variable signal error",
    "1031   Abnormal EPoller list",
    "1032   Connect to control server error",
    "1033   data send error",
    "1034   Close poll error",
    "1035   Wait poll time out",
    "1036   Cannot delete node index and stream index from Hash Table",
    "1037   Shutdown communication threads error",
    "1038   Internal close mailbox",
    "1039   Abnormal stream state",
    "1040   Invalid socket",
    "1041   No data in buffer",
    "1042   Cannot start thread",
    "1043   Cannot add entry to hash table",
    "1044   Cannot build association",
    "1045   Wrong plan id and plan node id",
    "1046   Close because release memory",
    "1047   control channel disconnect",
    "1048   data channel disconnect",
    "1049   Stream closed by remote",
    "1050   Stream closed by stream thread",
    "1051   Poll already closed by local",
    "1052   Poll already closed by remote",
    "1053   control channel connect failed",
    "1054   data channel connect failed",
    "1055   Stream connect failed",
    "1056   Stream rejected by remote",
    "1057   Connect wait time out",
    "1058   Wait quota failed",
    "1059   Wait poll unknow error",
    "1060   Peer address changed",
    "1061   GSS authentication failed",
    "1062   control channel send timeout",
    "1063   Not cluster internal IP",
};

const char* mc_comlib_strerror(int errnum)
{
    return comlib_error[errnum - 1000];
}

#endif  //_MC_ERRNO_COMLIB_H_
