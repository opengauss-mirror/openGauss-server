# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

#获取系统版本
function get_osname
{
    rhel_release="/etc/redhat-release"
    sles_release="/etc/SuSE-release"
    #获取RedHat的版本
    if [ -f "${rhel_release}" ]; then
        name="redhat-$(cat "${rhel_release}" | awk '{print $7}')"
    fi
    #获取SuSE的版本
    if [ -f "${sles_release}" ]; then
        name="suse-$(cat "${sles_release}" | grep "^VERSION" | awk '{print $3}')"
        name="${name}.$(cat "${sles_release}" | grep "^PATCHLEVEL" | awk '{print $3}')"
    fi
    #获取openEuler的版本
    if [ -f "/etc/openEuler-release" ]; then
        name=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    fi
    echo $name
}

