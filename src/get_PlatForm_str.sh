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
elif [ -f "/etc/centos-release" ]
then
	kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
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
	if [ "$version"x = "12"x ]
	then
		plat_form_str=suse12_"$cpu_bit"
	else
		plat_form_str=suse11_sp1_"$cpu_bit"
	fi
fi

##################################################################################
# euler platform 
# the result form like this: euleros2.0_sp8_aarch64
##################################################################################
if [ "$kernel"x = "euleros"x ]
then
	version=$(cat /etc/euleros-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
	plat_form_str=euleros2.0_"$version"_"$cpu_bit"
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
# kylin platform
# the result form like this: kylin_aarch64
##################################################################################
if [ "$kernel"x = "kylin"x ]
then
    plat_form_str=kylin_"$cpu_bit"
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

