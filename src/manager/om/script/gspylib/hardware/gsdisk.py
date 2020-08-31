# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : disk.py is a utility to do something for disk.
#############################################################################
import os
import subprocess
import sys
import psutil
import math

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsplatform import g_Platform

"""
Requirements:
1. getUsageSize(directory) -> get directory or file real size. Unit is byte.
2. getMountPathByDataDir(directory) -> get the input directory of the mount 
disk
3. getMountPathAvailSize(directory) -> get the avail size about the input 
directory of the mount disk. Unit MB
4. getDiskSpaceUsage(directory) -> get directory or file space size. Unit is 
byte.
5. getDiskInodeUsage(directory) -> get directory or file inode uage. Unit is 
byte.
6. getDiskMountType(directory) -> get the type about the input directory of 
the mount disk.
7. getDiskReadWritespeed(inputFile, outputFile, bs, count, iflag = '', 
oflag = '') -> get disk read/write speed
"""


class diskInfo():
    """
    function: Init the DiskUsage options
    """

    def __init__(self):
        self.mtabFile = g_Platform.getMtablFile()

    def getMountInfo(self, allInfo=False):
        """
        get mount disk information: device mountpoint fstype opts
        input: bool (physical devices and all others)
        output: list
        """
        return psutil.disk_partitions(allInfo)

    def getUsageSize(self, directory):
        """
        get directory or file real size. Unit is byte
        """
        cmd = ""
        try:
            cmd = "%s -l -R %s | %s ^- | %s '{t+=$5;} END {print t}'" % (
                g_Platform.getListCmd(), directory, g_Platform.getGrepCmd(),
                g_Platform.getAwkCmd())
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                return output.split('\t')[0].strip()
            else:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % str(output))
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd)

    # Mtab always keeps the partition information already mounted in the
    # current system.
    # For programs like fdisk and df, 
    # you must read the mtab file to get the partition mounting status in
    # the current system.
    def getMountPathByDataDir(self, datadir):
        """
        function : Get the disk by the file path
          input  : datadir   the file path
          output : device    disk
        """
        device = ""
        mountDisk = {}
        if not os.path.exists(datadir):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % datadir)
        try:
            datadir = os.path.realpath(datadir)
            with open(self.mtabFile, "r") as fp:
                for line in fp.readlines():
                    if line.startswith('none'):
                        continue
                    i_fields = line.split()
                    if len(i_fields) < 3:
                        continue
                    i_device = i_fields[0].strip()
                    i_mountpoint = i_fields[1].strip()
                    mountDisk[i_mountpoint] = [i_device, i_mountpoint]

            mountList = mountDisk.keys()
            sorted(mountList, reverse=True)
            for mount in mountList:
                i_mountpoint = mountDisk[mount][1]
                if (i_mountpoint == '/'):
                    i_mount_dirlst = ['']
                else:
                    i_mount_dirlst = i_mountpoint.split('/')
                data_dirlst = datadir.split('/')
                if len(i_mount_dirlst) > len(data_dirlst):
                    continue
                if (i_mount_dirlst == data_dirlst[:len(i_mount_dirlst)]):
                    device = mountDisk[mount][0]
                    break

        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] +
                            " disk mount." + "Error: %s" % str(e))
        return device

    # Mtab always keeps the partition information already mounted in the
    # current system.
    # For programs like fdisk and df, 
    # you must read the mtab file to get the partition mounting status in
    # the current system.
    def getMountPathAvailSize(self, device, sizeUnit='MB'):
        """
        function : Get the disk size by the file path
          input  : device    the file path 
                 : sizeUnit  byte, GB, MB, KB
          output : total     disk size
        """
        if (not os.path.exists(device)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % device)
        try:
            dev_info = os.statvfs(device)
            if (sizeUnit == 'GB'):
                total = dev_info.f_bavail * dev_info.f_frsize // (
                        1024 * 1024 * 1024)
            elif (sizeUnit == 'MB'):
                total = dev_info.f_bavail * dev_info.f_frsize // (1024 * 1024)
            elif (sizeUnit == 'KB'):
                total = dev_info.f_bavail * dev_info.f_frsize // 1024
            else:
                total = dev_info.f_bavail * dev_info.f_frsize
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] + " disk size."
                            + "Error: %s" % str(e))
        return total

    # Mtab always keeps the partition information already mounted in the
    # current system.
    # For programs like fdisk and df, 
    # you must read the mtab file to get the partition mounting status in
    # the current system.
    def getDiskSpaceUsage(self, path):
        """
        function : Get the disk usage by the file path
                method of calculation:
                    Total capacity (KB)=f_bsize*f_blocks/1024 [1k-blocks]
                    Usage (KB)= f_bsize*(f_blocks-f_bfree)/1024 [Used]
                    Valid capacity (KB) = f_bsize*f_bavail/1024 [Available]
                    Usage (%) = Usage/(Usage + Valid capacity) *100 [Use%]
          input  : path      the file path
          output : percent
        """
        percent = 0
        if (not os.path.exists(path)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % path)
        try:
            dev_info = os.statvfs(path)
            used = dev_info.f_blocks - dev_info.f_bfree
            valueable = dev_info.f_bavail + used
            percent = math.ceil((float(used) / valueable) * 100)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] + " disk space."
                            + "Error: %s" % str(e))
        return float(percent)

    def getDiskSpaceForShrink(self, path, delta):
        """
        function : Get the disk usage by the file path for Shrink
          input  : path      the file path and deltasize
          output : percent
        """
        percent = 0
        if (not os.path.exists(path)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % path)
        try:
            dev_info = os.statvfs(path)
            used = (dev_info.f_blocks - dev_info.f_bfree) * dev_info.f_bsize
            valueable = dev_info.f_bavail * dev_info.f_bsize + used + delta
            percent = math.ceil((float(used) // valueable) * 100)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] + " disk space."
                            + "Error: %s" % str(e))
        return float(percent)

    def getDiskInodeUsage(self, Path):
        """
        function : Get the inode by the file path
        input  : Path     the file path
        output : percent
        """
        percent = 0
        if (not os.path.exists(Path)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] % Path)
        try:
            dev_info = os.statvfs(Path)
            used = dev_info.f_files - dev_info.f_ffree
            valueable = dev_info.f_favail + used
            percent = math.ceil((float(used) // valueable) * 100)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] + " disk Inode."
                            + "Error: %s" % str(e))
        return float(percent)

    def getDiskMountType(self, device):
        """
        function : Get the mount type by device
        input  : device   eg:/dev/pts
        output : fstype   device type
        """
        fstype = ""
        try:

            with open(self.mtabFile, "r") as fp:
                for line in fp.readlines():
                    if line.startswith('#'):
                        continue
                    i_fields = line.split()
                    if len(i_fields) < 3:
                        continue
                    i_device = i_fields[0].strip()
                    i_fstype = i_fields[2].strip()
                    if i_device == device:
                        fstype = i_fstype
                        break
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"]
                            + " disk mount type." + "Error: %s" % str(e))
        return fstype

    def getDiskReadWritespeed(self, inputFile, outputFile, bs, count, iflag='',
                              oflag=''):
        """
        function : Get the disk read or write rate
        input  : inputFile   
               : outputFile  
               : bs
               : count
               : iflag
               : oflag
        output : speed 
        """
        try:
            cmd = "%s if=%s of=%s bs=%s count=%s " % (
                g_Platform.getDdCmd(), inputFile, outputFile, bs, count)
            if iflag:
                cmd += "iflag=%s " % iflag
            if oflag:
                cmd += "oflag=%s " % oflag

            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                output = output.split("\n")
                resultInfolist = output[2].strip().split(",")
                if ((resultInfolist[2]).split()[1] == "KB/s"):
                    speed = float((resultInfolist[2]).split()[0]) * 1024
                elif ((resultInfolist[2]).split()[1] == "MB/s"):
                    speed = float((resultInfolist[2]).split()[0]) * 1024 * 1024
                elif ((resultInfolist[2]).split()[1] == "GB/s"):
                    speed = float(
                        (resultInfolist[2]).split()[0]) * 1024 * 1024 * 1024
                elif ((resultInfolist[2]).split()[1] == "TB/s"):
                    speed = float((resultInfolist[2]).split()[
                                      0]) * 1024 * 1024 * 1024 * 1024
                else:
                    speed = float((resultInfolist[2]).split()[0])
                return speed
            else:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % str(output))
        except Exception as e:
            raise Exception(
                ErrorCode.GAUSS_504["GAUSS_50406"] + "Error:\n%s" % str(e))


g_disk = diskInfo()
