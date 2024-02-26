#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2019. All rights reserved
#
# description: Acording plat form, get the string info, like "redhat6.4_x86_64".
# return:  $plat_form_str : we support the platform and put out $plat_form_str
#          "Failed" : the plat form, not supported
# date: 2015-8-22
# version: 1.0
#
# *************************************************************************
set -e

##############################################################################################
# common paremeters:
# lsb_release and uname both suit almost all linux platform, including Redhat,CentOS,SuSE,Debian and so on.
##############################################################################################
# get os name
kernel=""
if [ -f "/etc/euleros-release" ]
then
    kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/openEuler-release" ]
then
    kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/FusionOS-release" ]
then
    kernel=$(cat /etc/FusionOS-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/centos-release" ]
then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/kylin-release" ]
then
    kernel=$(cat /etc/kylin-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/CSIOS-release" ]
then
    kernel=$(cat /etc/CSIOS-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
else
    kernel=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z)
fi

## to solve kernel="name=openeuler"
if echo $kernel | grep -q 'openeuler'
then
    kernel="openeuler"
fi

# get cpu bit
cpu_bit=$(uname -p)

# the result info
plat_form_str=""

##################################################################################
# redhat platform
# the result form like this: redhat6.4_x86_64
##################################################################################
if [ "$kernel"x = "red"x ]
then
    plat_form_str=redhat6.4_"$cpu_bit"
fi

##################################################################################
# fedora platform
# the result form like this: redhat6.4_x86_64
##################################################################################
if [ "$kernel"x = "fedora"x ]
then
    plat_form_str=redhat6.4_"$cpu_bit"
fi

##################################################################################
# suse platform 
# the result form like this: suse11_sp1_x86_64
##################################################################################
if [ "$kernel"x = "suse"x ]
then
    version=$(lsb_release -r | awk -F ' ' '{print $2}')
    plat_form_str=suse${version%%\.*}_sp${version##*\.}_"$cpu_bit"
fi

##################################################################################
# euler platform 
# the result form like this: euleros2.0_sp8_aarch64
##################################################################################
if [ "$kernel"x = "euleros"x ]
then
    major_version=$(cat /etc/euleros-release | awk  '{print $3}')
    minor_version=$(cat /etc/euleros-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
    minor_version=${minor_version%x86_64}
    minor_version=${minor_version%aarch64}
    plat_form_str=euleros${major_version}_"$minor_version"_"$cpu_bit"
fi

##################################################################################
# deepin platform 
# the result form like this: deepin_aarch64
##################################################################################
if [ "$kernel"x = "deepin"x ]
then
    if [ X"$cpu_bit" = X"unknown" ]
    then
        cpu_bit=$(uname -m)
    fi
    plat_form_str=deepin15.2_"$cpu_bit"
fi
##################################################################################
# centos7.6_x86_64 platform
# centos7.5+aarch64 platform 
# the result form like this: centos7.6_x86_64 or centos_7.5_aarch64
##################################################################################
if [ "$kernel"x = "centos"x ]
then
    if [ X"$cpu_bit" = X"aarch64" ]
    then
        plat_form_str=centos_7.5_aarch64
    else
        plat_form_str=centos7.6_"$cpu_bit"
    fi
fi


##################################################################################
# openeuler platform
# the result form like this: openeuler_aarch64
##################################################################################
if [ "$kernel"x = "openeuler"x ]
then
    plat_form_str=openeuler_"$cpu_bit"
fi


##################################################################################
# fusionos platform
# the result form like this: fusionos_x86_64
##################################################################################
if [ "$kernel"x = "fusionos"x ]
then
    plat_form_str=fusionos_"$cpu_bit"
fi


##################################################################################
# kylin platform
# the result form like this: kylin_aarch64
##################################################################################
if [ "$kernel"x = "kylin"x ];then
    plat_form_str=kylinv10_sp1_"$cpu_bit"
fi

##################################################################################
# ubuntu platform
# the result form like this: ubuntu_x86_64
##################################################################################
if [ "$kernel"x = "ubuntu"x ]
then
    plat_form_str=ubuntu18.04_"$cpu_bit"
fi

##################################################################################
# the modification here will also lead to the synchronous modification of the 3rd-party compilation library path
# PR link : https://gitee.com/opengauss/openGauss-third_party/pulls/130
# redflag platform  
# the result form like this: asianux_x86_64
##################################################################################
if [ "$kernel"x = "redflag"x ]
then
    plat_form_str=asianux7.6_"$cpu_bit"
fi


##################################################################################
# the modification here will also lead to the synchronous modification of the 3rd-party compilation library path
# PR link : https://gitee.com/opengauss/openGauss-third_party/pulls/130
# asianux platform
# the result form like this: asianux_aarch64
##################################################################################
if [ "$kernel"x = "asianux"x ]
then
    plat_form_str=asianux7.5_"$cpu_bit"
fi

##################################################################################
# csios platform
# the result form like this: csios_x86_64
##################################################################################
if [ "$kernel"x = "csios"x ]
then
    plat_form_str=csios_"$cpu_bit"
fi

##################################################################################
#
# other platform 
#
##################################################################################
if [ -z "$plat_form_str" ]
then
    echo "Failed"
else
    echo $plat_form_str
fi

