# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

# 清理环境--消息队列、共享内存、信号量 
function cleanEnv_ipcs
{
    user=$1
    
    if [ -z $user ] ;then
        echo -e "\033[31m[CleanEnv--ipcs] ERROR: Need 1 parameter !!!\033[0m"
        return 1
    fi
    
    echo "[CleanEnv--ipcs] `date \"+%Y-%m-%d %T\"` START"
    
    # 清理消息队列
    echo '  Removing "Q(message queues)" created by "'$user'" ...'
    ipcs -q|grep "\b${user}\b"|awk '{print $2}'|xargs -t -I {} ipcrm -q {}
    
    # 清理共享内存
    echo '  Removing "M(shared memory segments)" created by "'$user'" ...'
    ipcs -m|grep "\b${user}\b"|awk '{print $2}'|xargs -t -I {} ipcrm -m {}
    
    # 清理信号量
    echo '  Removing "S(semaphore arrays)" created by "'$user'" ...'
    ipcs -s|grep "\b${user}\b"|awk '{print $2}'|xargs -t -I {} ipcrm -s {}
    
    # 检查清理结果
    res="$(ipcs -qms|grep "\btester1\b\|key\|----")"
    if [ $(printf "%s\n" "$res"|wc -l) -eq 6 ] ; then
        echo "[CleanEnv--ipcs] `date \"+%Y-%m-%d %T\"` SUCCESS !!!"
        return 0
    else
        echo -e "\033[31m[CleanEnv--ipcs] `date \"+%Y-%m-%d %T\"` FAIL!!!\033[0m"
        printf "%s\n" "$res"
        return 1
    fi
}
