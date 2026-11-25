# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

append_timestamp ()
{
    trans_name=$1
    while IFS= read -r line; do
        printf '[%s %s] [%s] %s\n' "$(date +'%y_%m_%d')" "$(date +'%H_%M_%S')" "$trans_name" "$line";
    done
}

#gsql ${@:2} 2>&1 | append_timestamp "$1"

