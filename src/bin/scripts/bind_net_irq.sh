#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# bind_net_irq.sh
#    Bind network device's irq affinity
#
# IDENTIFICATION
#    src/bin/scripts/bind_net_irq.sh
#
#-------------------------------------------------------------------------

export LC_ALL=en_US.UTF-8
net_cores=$1

#check euleros stop sysmonitor
os=$(cat /etc/*release | grep "NAME=" |  head -n 1 | cut -d '=' -f 2 | tr -d '"' | tr A-Z a-z)

if [ "$os"x = "euleros"x ]
then
    service sysmonitor stop
fi

service irqbalance stop


core_no=$(lscpu|grep "CPU(s):"|awk {'print $2'}| head -n 1)
numa_node=$(lscpu|grep 'NUMA node(s):'|awk {'print $3'})
cores_per_numa=$(($core_no / $numa_node))


declare net_irq_no
declare net_devices_array
declare irq_array_net
declare net_dev_no=0

#check OS Parameters of NUMA and SMMU
function check_os()
{
    echo "Platform number of cores:" $core_no

    if [ $numa_node -ne 4 ]
    then
         echo "Warning: Number of NUMA nodes is not matched, please Disable DIE interleave in BIOS"
    fi

    smmu=$(ps aux|grep smmu)

    if [[ $smmu == *"arm-smmu"* ]]
    then
        echo $smmu
        echo "Error: SMMU enabled, please Disable SMMU in BIOS"
        exit
    fi
}


#get all the network active devices
function get_active_devices()
{
    net_dev_all_no=$(ip link | awk -F: '$0 !~ "lo|vir|wl|^[^0-9]"{print $2;getline}' |wc -l)
    net_devices=$(ip link | awk -F: '$0 !~ "lo|vir|wl|^[^0-9]"{print $2;getline}')
    net_devices_all=($net_devices)
    net_devices_array=()
    for((n=0; n<$net_dev_all_no; ++n))
    do
    if_speed=$(ethtool ${net_devices_all[$n]}|grep Speed|awk {'print $2'}|grep -o '[0-9]*')
        if [[ $if_speed -ge 10000 ]]
        then
            net_devices_array[$net_dev_no]=${net_devices_all[$n]}
            net_dev_no=$(($net_dev_no+1))
        fi
    done
}

#get mellanox the network devices
function get_mellanox_devices()
{
    net_dev_no=$(lspci |grep -i ethernet |wc -l)
    net_devices=$(lspci |grep -i ethernet | awk {'print $1'})
    IFS=$'\n' read -d '' -a net_devices_array <<< "$net_devices"
}

#get the network interface irq
function get_irq_net()
{
    net_name=$1
    net_irq_no=$(cat /proc/interrupts | grep $net_name | awk  {'print $1'} | wc -l)
    irq=$(cat /proc/interrupts | grep $net_name | awk  {'print $1'} | sed ':a;N;s/\n//g;ba')
    IFS=':' read -r -a irq_array_net <<< "$irq"
}


#Tune network for offloading traffic and set coalesce IRQ to combined spcific number
function net_tune()
{
    net_dev=$1
    action=$2

    if [ -z $net_dev ]
    then
        echo "Error: unknown network interface!"
        exit
    fi

    if [ -z $action ]
    then
        echo "enable network offloading traffic as default!"
        action="on"
    fi

    ethtool -K $net_dev tso $action #off
    ethtool -K $net_dev lro $action #off
    ethtool -K $net_dev gro $action #off
    ethtool -K $net_dev gso $action #off


    combined_ret=$({ ethtool -L $net_dev combined 48 > error.txt; } 2>&1)
    if [[ $combined_ret == *"Invalid argument"* ]]
    then
        echo "Warning: Can not set the network irq combined:" $net_dev "--" $combined_ret
        echo "Hint: If using Hi1822, please update your Firmware"
    fi

    coalesce_ret=$({ ethtool -C $net_dev rx-usecs 512 tx-usecs 512 > error.txt; } 2>&1)
    if [[ $coalesce_ret == *"Invalid argument"* ]]
    then
        echo "Warning: Can not set the network coalesce:" $net_dev "--" $coalesce_ret
    fi
}


#Tune specific network interface
function tune_net_if()
{
    net_name=$1
    echo "tune $net_name"
    net_tune $net_name
    net_irq_no=$(ethtool -l $net_name|grep Combined|tail -n 1|awk {'print $2'})
    if [[ ! -n "$net_cores" ]] && [[ "$net_irq_no" -ge 48 ]]
    then
        net_cores=12
    elif [[ ! -n "$net_cores" ]]
    then
        net_cores=$net_irq_no
    fi
    echo "Assigned $net_cores cores to $net_name IRQ"
    #wait 2 seconds for the net_tune done
    sleep 2
}

#check and varify whether IRQ have been bind to specific cores in NUMA nodes
function check_irq()
{
    device_name=$1
    echo "NUMA nodes core affinity for $device_name IRQs"

    for((i=0;i<$numa_node;++i))
    do
        printf "%-10s " "NUMA $i"
    done
    printf "\n"
    for((i=0; i<net_irq_no; i=i+$numa_node))
    do
        line=""
        for((j=0; j<numa_node; ++j))
        do
            irq_offset=$(($i+$j))
            line=$(cat /proc/irq/${irq_array_net[$irq_offset]}/effective_affinity_list)
            printf "%-10s " "$line"
        done
        printf "\n"
    done
}


check_os
net_irq_total=0
get_active_devices
for((k=0;k<net_dev_no;++k))
do
    get_irq_net ${net_devices_array[k]}
    net_irq_total=$(($net_irq_total+$net_irq_no))
done

if [ $net_irq_total -eq 0 ]
then
    get_mellanox_devices
fi


declare node=0
for((k=0;k<net_dev_no;++k))
do
    tune_net_if ${net_devices_array[$k]}
    get_irq_net ${net_devices_array[k]}
    if [ $net_irq_no -eq 0 ]
    then
        echo "Can't not bind ${net_devices_array[$k]}'s irqs, No irqs in ${net_devices_array[$k]}"
        continue;
    fi

    for((i=0; i<net_irq_no; ++i))
    do
        echo $(($(($(($i+1))*$cores_per_numa-$(($i%${net_cores}/$numa_node))-1))%core_no)) > /proc/irq/${irq_array_net[i]}/smp_affinity_list
    done
    echo "Bind network " ${net_devices_array[$k]} " irq success!!"
    check_irq ${net_devices_array[$k]}
done
