# -*- coding:utf-8 -*-
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

import sys
import subprocess
import os
import pwd
import grp
import shutil
import _thread as thread
import time
import stat

localDirPath = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, localDirPath + "/../../../lib")
try:
    import psutil
except ImportError as e:
    # mv psutil mode .so file by python version
    pythonVer = sys.version[:3]
    psutilLinux = os.path.join(localDirPath,
                               "./../../../lib/psutil/_psutil_linux.so")
    psutilPosix = os.path.join(localDirPath,
                               "./../../../lib/psutil/_psutil_posix.so")
    psutilLinuxBak = "%s_%s" % (psutilLinux, pythonVer)
    psutilPosixBak = "%s_%s" % (psutilPosix, pythonVer)

    glo_cmd = "rm -rf '%s' && cp -r '%s' '%s' " % (psutilLinux,
                                                   psutilLinuxBak,
                                                   psutilLinux)
    glo_cmd += " && rm -rf '%s' && cp -r '%s' '%s' " % (psutilPosix,
                                                        psutilPosixBak,
                                                        psutilPosix)
    psutilFlag = True
    for psutilnum in range(3):
        (status_mvPsutil, output_mvPsutil) = subprocess.getstatusoutput(
            glo_cmd)
        if status_mvPsutil != 0:
            psutilFlag = False
            time.sleep(1)
        else:
            psutilFlag = True
            break
    if not psutilFlag:
        print("Failed to execute cmd: %s. Error:\n%s" % (glo_cmd,
                                                         output_mvPsutil))
        sys.exit(1)
    # del error import and reload psutil
    del sys.modules['psutil._common']
    del sys.modules['psutil._psposix']
    import psutil
sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsplatform import g_Platform, findCmdInPath


class fileManage():
    """
    Class to handle OS file operations
    """
    SHELL_CMD_DICT = {
        "deleteFile": "(if [ -f '%s' ];"
                      "then rm -f '%s';fi)",
        "deleteLibFile": "cd %s && ls | grep -E '%s'|"
                         "xargs rm -f",
        "cleanDir": "(if [ -d '%s' ];then rm -rf "
                    "'%s'/* && cd '%s' && ls -A | "
                    "xargs rm -rf ; fi)",
        "execShellFile": "sh %s",
        "getFullPathForShellCmd": "which %s",
        "deleteDir": "(if [ -d '%s' ];then rm "
                     "-rf '%s';fi)",
        "deleteLib": "(if [ -e '%s' ];then rm "
                     "-rf '%s';fi)",
        "createDir": "(if [ ! -d '%s' ]; "
                     "then mkdir -p '%s' -m %s;fi)",
        "createFile": "touch '%s' && chmod %s '%s'",
        "deleteBatchFiles": "rm -f %s*",
        "compressTarFile": "cd '%s' && tar -cf "
                           "'%s' %s && chmod %s '%s'",
        "decompressTarFile": "cd '%s' && tar -xf '%s' ",
        "copyFile": " cp -rf %s %s ",
        "sshCmd": "pssh -s -H %s 'source %s;%s'",
        "renameFile": "(if [ -f '%s' ];then mv '%s' "
                      "'%s';fi)",
        "cleanFile": "if [ -f %s ]; then echo '' > "
                     "%s; fi",
        "exeRemoteShellCMD": "pssh -s -H %s 'source %s;%s'",
        "exeRemoteShellCMD1": "pssh -s -H %s \"%s\"",
        "userExeRemoteShellCmd": "su - %s -c \"pssh -s -H %s "
                                 "'%s'\"",
        "checkUserPermission": "su - %s -c \"cd '%s'\"",
        "getFileTime": "echo $[`date +%%s`-`stat -c "
                       "%%Y %s`]",
        "scpFileToRemote": "pscp -H '%s' '%s' '%s'",
        "scpFileFromRemote": "pssh -s -H '%s' \"pscp -H "
                             "'%s' '%s' '%s' \"",
        "findfiles": "cd %s && find . "
                     "-type l -print",
        "copyFile1": "(if [ -f '%s' ];then cp "
                     "'%s' '%s';fi)",
        "copyFile2": "(if [ -f '%s' ] && [ ! -f "
                     "'%s' ];then cp '%s' '%s';fi)",
        "copyRemoteFile": "(if [ -d '%s' ];then pssh "
                          "-s -H '%s' \"pscp -H '%s' "
                          "'%s' '%s' \";fi)",
        "cleanDir1": "(if [ -d '%s' ]; then cd "
                     "'%s' && rm -rf '%s' && "
                     "rm -rf '%s' && cd -; fi)",
        "cleanDir2": "(if [ -d '%s' ]; then "
                     "rm -rf '%s'/* && cd '%s' && "
                     "ls -A | xargs rm -rf && "
                     "cd -; fi)",
        "cleanDir3": "rm -rf '%s'/* && cd '%s' && "
                     "ls -A | xargs rm -rf && "
                     "cd - ",
        "cleanDir4": "rm -rf %s/*",
        "checkNodeConnection": "ping %s -i 1 -c 3 |grep ttl |"
                               "wc -l",
        "overWriteFile": "echo '%s' > '%s'",
        "physicMemory": "cat /proc/meminfo | "
                        "grep MemTotal",
        "findFile": "(if [ -d '%s' ]; then "
                    "find '%s' -type f;fi)",
        "unzipForce": "unzip -o '%s' -d '%s'",
        "killAll": findCmdInPath("killall") + " %s",
        "sleep": "sleep %s",
        "softLink": "ln -s '%s' '%s'",
        "findwithcd": "cd %s && find ./ -name %s",
        "installRpm": "rpm -ivh --nofiledigest %s "
                      "--nodeps --force --prefix=%s",
        "changeMode": "chmod %s %s",
        "checkPassword": "export LC_ALL=C; "
                         "chage -l %s | "
                         "grep -i %s"
    }

    def __init__(self):
        """
        constructor
        """
        pass

    def createFile(self, path, overwrite=True, mode=None):
        """
        function: create file and set the permission
        input:
            path: the file path.
            overwrite: if file already exists and this parameter is true,
            we can overwrtie it.
            mode: Specify file permissions, type is int and start with 0.
            ex: 0700
        output:
            return true or false.
        """
        try:
            if overwrite:
                cmd = g_Platform.getCreateFileCmd(path)
                if mode:
                    cmd += "; %s" % g_Platform.getChmodCmd(str(mode), path)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(output + " The cmd is %s" % cmd)
            else:
                # create file by python API
                if mode:
                    os.mknod(path, mode)
                else:
                    os.mknod(path)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"] % path +
                            " Error:\n%s" % str(e))

        return True

    def createFileInSafeMode(self, filePath, mode=stat.S_IWUSR | stat.S_IRUSR):
        """
        Call this method before open(filePath) functions,
        if it may create a new file.
        This method guarantees a 0o600 file is created
        instead of an arbitrary one.
        """
        if os.path.exists(filePath):
            return
        try:
            os.mknod(filePath, mode)
        except IOError as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"] % filePath +
                            " Error:\n%s." % str(e))

    def removeFile(self, path, cmdType="shell"):
        """
        function: remove a file
        input: the path of file(include file name)
        output: return true or false
        """
        if cmdType == "python":
            # no file need remove.
            if not os.path.exists(path):
                return True
            # check if is a file.
            if not os.path.isfile(path):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % path)
            try:
                # remove file.
                os.remove(path)
            except Exception:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] % path)
        else:
            # Support* for fuzzy matching
            if "*" in path:
                path = self.withAsteriskPath(path)
                cmd = g_Platform.getRemoveCmd('file') + path
            else:
                cmd = g_Platform.getRemoveCmd('file') + "'" + path + "'"
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] % path
                                + " Error:\n%s" % output
                                + " The cmd is %s" % cmd)
        return True

    def moveFile(self, src, dest, overwrite=True):
        """
        function: move a file
        input:
            src: the dir of file
            dest: the dir which want to move
        output:
            return true or false
        """
        # check if can overwrite
        if os.path.exists(dest) and not overwrite:
            raise Exception(ErrorCode.GAUSS_501["GAUSS_50102"] % (
                "parameter overwrite", dest))
        try:
            if overwrite:
                cmd = g_Platform.getMoveFileCmd(src, dest)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(output + "The cmd is %s" % cmd)
            else:
                # move file
                shutil.move(src, dest)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50232"] % (src, dest) +
                            " Error:\n%s" % str(e))

        return True

    def readFile(self, filename, keyword="", rows=0):
        """
        function: read the content of a file
        input:
            filename: the name and path of the file
            keyword: read line include keyword
            rows: the row number, which want to read
            offset: keep the parameter, but do nothing
        output:list
        """
        listKey = []
        strRows = ""
        allLines = []
        # check if file exists.
        if not os.path.exists(filename):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % filename)
        try:
            with open(filename, 'rb') as fp:
                for line in fp:
                    allLines.append(line.decode("utf-8"))
        except Exception:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] % filename)
        # get keyword lines
        if keyword != "":
            for line in allLines:
                flag = line.find(keyword)
                if flag >= 0:
                    listKey.append(line)
        # get content of row
        if rows:
            if not str(rows).isdigit():
                raise Exception
            if rows > 0:
                row_num = rows - 1
            else:
                row_num = rows
            try:
                if row_num < (len(allLines)):
                    strRows = allLines[row_num]
            except Exception:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] % (
                        "the %s line of the file [%s]" % (rows, filename)))
        # check which needs return
        if keyword != "" and rows != 0:
            return [strRows]
        if keyword != "" and rows == 0:
            return listKey
        if keyword == "" and rows != 0:
            return [strRows]
        if keyword == "" and rows == 0:
            return allLines

    def writeFile(self, path, context=None, mode="a+"):
        """
        function: write content in a file
        input:
            path: the name and path of the file
            context: the content, which want to write
            mode: the write mode
        output:
        """
        lock = thread.allocate_lock()
        if context is None:
            context = []
        # check if not exists.
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        # check if is a file.
        if not os.path.isfile(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % path)
        # if no context, return
        if not context:
            return False
        self.createFileInSafeMode(path)
        with open(path, mode) as fp:
            fp.writelines(line + os.linesep for line in context)
            lock.acquire()
            try:
                # write context.
                fp.flush()
            except Exception as e:
                lock.release()
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % path +
                                "Error:\n%s" % str(e))
            lock.release()
        return True

    def withAsteriskPath(self, path):
        """
        function: deal with the path with *
        input: the path to deal with
        output: cmd
        """
        path_dirList = os.path.realpath(path).split(os.path.sep)[1:]
        path = "'"
        for dirName in path_dirList:
            if "*" in dirName:
                dirPath = "'" + os.path.sep + dirName + "'"
            else:
                dirPath = os.path.sep + dirName
            path += dirPath
        if path[-1] == "'":
            path = path[:-1]
        else:
            path += "'"
        return path

    def changeMode(self, mode, path, recursive=False, cmdType="shell",
                   retryFlag=False, retryTime=15, waiteTime=1):
        """
        function: change permission of file
        input:
            cmdType: user shell or python
            mode:permission value, Type is int and start with 0. ex: 0700
            path:file path
            recursive: recursive or not
        output:
        """
        try:
            # do with shell command.
            if cmdType == "shell":
                if "*" in path:
                    path = self.withAsteriskPath(path)
                else:
                    path = "'" + path + "'"
                cmd = g_Platform.getChmodCmd(str(mode), path, recursive)
                if retryFlag:
                    self.retryGetstatusoutput(cmd, retryTime, waiteTime)
                else:
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(ErrorCode.GAUSS_501[
                                        "GAUSS_50107"] % path +
                                        " Error:\n%s." % output +
                                        "The cmd is %s" % cmd)
            # do with python API. If the name has special characters.
            else:
                os.chmod(path, mode)
        except Exception as e:
            raise Exception(str(e))
        return True

    def changeOwner(self, user, path, recursive=False, cmdType="shell",
                    retryFlag=False, retryTime=15, waiteTime=1):
        """
        function: change the owner of file
        input: cmdType, user, path, recursive
        output: return true
        """
        try:
            # get uid and gid by username.
            userInfo = pwd.getpwnam(user)
            uid = userInfo.pw_uid
            gid = userInfo.pw_gid
            group = grp.getgrgid(gid).gr_name
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50308"] +
                            " Error:\n%s" % str(e))
        try:
            # do with shell command.
            if cmdType == "shell":
                if "*" in path:
                    path = self.withAsteriskPath(path)
                else:
                    path = "'" + path + "'"
                cmd = g_Platform.getChownCmd(user, group, path, recursive)
                if retryFlag:
                    self.retryGetstatusoutput(cmd, retryTime, waiteTime)
                else:
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(output + " The cmd is %s" % cmd)
            # do with python API. If the name has special characters.
            else:
                os.chown(path, uid, gid)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_501["GAUSS_50106"] % path +
                            " Error:\n%s." % str(e))
        return True

    def retryGetstatusoutput(self, cmd, retryTime, sleepTime):
        """
        function : exectue commands, if fail ,then retry it.
        input : cmd, waitTimes, retryTimes
        output: NA
        """
        countNum = 0
        (status, output) = subprocess.getstatusoutput(cmd)
        while countNum < retryTime:
            if status != 0:
                sleepCmd = "sleep %s" % sleepTime
                subprocess.getstatusoutput(sleepCmd)
                (status, output) = subprocess.getstatusoutput(cmd)
                countNum = countNum + 1
            else:
                break
        if status != 0:
            raise Exception(ErrorCode.GAUSS_501["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)

    def createDirectory(self, path, overwrite=True, mode=None):
        """
        function: create a directory
        input: path, overwrite
        output: true
        """
        try:
            if os.path.exists(path) and not overwrite:
                raise Exception(ErrorCode.GAUSS_501["GAUSS_50102"] % (
                    "parameter overwrite", path))
            if overwrite:
                cmd = g_Platform.getMakeDirCmd(path, overwrite)
                if mode:
                    cmd += "; %s" % g_Platform.getChmodCmd(str(mode), path)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(output + " The cmd is %s" % cmd)
            if not overwrite:
                if mode:
                    os.mkdir(path, mode)
                else:
                    os.mkdir(path)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50208"] % path +
                            " Error:\n%s" % str(e))
        return True

    def cleanDirectoryContent(self, path):
        """
        function: clean the content in a directory,
        but do not remove directory.
        input:path
        output:true
        """
        rm_Dirfile = "cd %s && ls | xargs -n 100000" % (path)
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        cmd = "%s %s && %s '%s'/.[^.]*" % (rm_Dirfile, g_Platform.getRemoveCmd(
            "directory"), g_Platform.getRemoveCmd("directory"), path)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50209"] % (
                    "content in the directory %s " % path) +
                    " Error:\n%s." % output + "The cmd is %s" % cmd)
        return True

    def removeDirectory(self, path):
        """
        function: remove the content in a directory
        input:path
        output:true
        """
        if "*" in path:
            path = self.withAsteriskPath(path)
            cmd = "%s %s" % (g_Platform.getRemoveCmd("directory"), path)
        else:
            cmd = "%s '%s'" % (g_Platform.getRemoveCmd("directory"), path)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50209"] % path +
                            " Error:\n%s." % output + "The cmd is %s" % cmd)
        return True

    def moveDirectory(self, src, dest):
        """
        function:move the content in a directory
        input:src, dest
        output:true
        """
        if not os.path.exists(src):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % src)
        if not os.path.exists(dest):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % dest)
        cmd = g_Platform.getMoveCmd(src, dest)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)
        return True

    def getDirectoryList(self, path, keywords="", recursive=False):
        """
        function:give the list of file in the directory
        input:path, keywords, recursive
        output:list
        """
        list_Dir = []
        try:
            if keywords == "":
                if recursive:
                    cmd = "%s -R '%s'" % (g_Platform.getListCmd(), path)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(output + "\nThe cmd is %s" % cmd)
                    list_Dir = output.split('\n')
                else:
                    list_Dir = os.listdir(path)
            else:
                if recursive:
                    cmd = "%s -R '%s' |%s -E '%s'" % (
                        g_Platform.getListCmd(), path,
                        g_Platform.getGrepCmd(), keywords)
                else:
                    cmd = "%s '%s' |%s -E '%s'" % (
                        g_Platform.getListCmd(), path,
                        g_Platform.getGrepCmd(), keywords)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0 and output != "":
                    raise Exception(output + "\nThe cmd is %s" % cmd)
                else:
                    list_Dir = output.split('\n')
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % (
                    "the list of %s" % path) + " Error:\n%s" % str(e))
        while '' in list_Dir:
            list_Dir.remove('')
        return list_Dir

    def cpFile(self, src, dest, cmdType="shell", skipCheck=False):
        """
        function: copy a file
        input:src, dest, cmdType
        output:true
        """
        if skipCheck:
            if not os.path.exists(src):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % src)
            if not os.path.exists(os.path.dirname(dest)):
                raise Exception(ErrorCode.GAUSS_502[
                                    "GAUSS_50201"] % os.path.dirname(dest))
        try:
            if cmdType != "shell":
                shutil.copy(src, dest)
            else:
                cmd = g_Platform.getCopyCmd(src, dest, "directory")
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(output + "\nThe cmd is %s" % cmd)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50214"] % src +
                            " Error:\n%s" % str(e))
        return True

    def findFile(self, path, keyword, choice='name'):
        """
        function:find a file by name or size or user
        input:path, keyword, choice, type
        output:NA
        """
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        cmd = "%s '%s' -%s %s " % (g_Platform.getFindCmd(), path,
                                   choice, keyword)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % (
                    "the files of path %s" % path) + " Error:\n%s" % output
                            + "\nThe cmd is %s" % cmd)
        list_File = output.split('\n')
        while '' in list_File:
            list_File.remove('')
        return list_File

    def compressFiles(self, tarName, dirPath):
        """
        function:compress directory to a package
        input:tarName, directory
        output:NA
        """
        cmd = g_Platform.getCompressFilesCmd(tarName, dirPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50227"] % cmd +
                            " Error:\n%s" % output)

    def decompressFiles(self, srcPackage, dest):
        """
        function:decompress package to files
        input:srcPackage, dest
        output:NA
        """
        if not os.path.exists(srcPackage):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % srcPackage)
        if not os.path.exists(dest):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % dest)
        cmd = g_Platform.getDecompressFilesCmd(srcPackage, dest)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50231"] % srcPackage +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def compressZipFiles(self, zipName, dirPath):
        """
        function:compress directory to a package
        input:zipName, directory
        output:NA
        """
        if not os.path.exists(dirPath):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % dirPath)
        cmd = g_Platform.getCompressZipFilesCmd(zipName, dirPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50227"] % cmd +
                            " Error:\n%s" % output)

    def decompressZipFiles(self, srcPackage, dest):
        """
        function:decompress package to files
        input:srcPackage, dest
        output:NA
        """
        if not os.path.exists(srcPackage):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % srcPackage)
        if not os.path.exists(dest):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % dest)
        cmd = g_Platform.getDecompressZipFilesCmd(srcPackage, dest)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50231"] % srcPackage +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def getfileUser(self, path):
        """
        function: get the info(username group) of a file
        input:path
        output:list of info
        """
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)

        user = pwd.getpwuid(os.stat(path).st_uid).pw_name
        group = grp.getgrgid(os.stat(path).st_gid).gr_name
        return user, group

    def replaceFileLineContent(self, oldLine, newLine, path):
        """
        function: replace the line in a file to a new line
        input:
        oldLine : Need to replace content
        newLine : Replaced content
        path
        output:NA
        """
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)

        cmd = g_Platform.getReplaceFileLineContentCmd(oldLine, newLine, path)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50223"] % path +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def checkIsInDirectory(self, fileName, directoryList):
        """
        function : Check if the file is in directoryList.
        input : String,[]
        output : []
        """
        try:
            isExist = False
            for onePath in directoryList:
                dirName = os.path.normpath(fileName)
                isExist = False

                while dirName != "/":
                    if dirName == onePath:
                        isExist = True
                        break
                    dirName = os.path.dirname(dirName)

                if isExist:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50229"] % (
                        fileName, onePath))
        except Exception as e:
            raise Exception(str(e))
        return isExist

    def checkDirWriteable(self, dirPath):
        """
        function : Check if target directory is writeable for execute user.
        input : String,String
        output : boolean
        """
        # if we can touch a tmp file under the path, it is true;
        return os.access(dirPath, os.W_OK)

    def checkFilePermission(self, filename, isread=False, iswrite=False,
                            isexecute=False):
        """
        Function : check file: 1.exist 2. isfile 3. permission
        Note     : 1.You must check that the file exist and is a file.
                   2.You can choose whether to check the file's
                   permission:readable/writable/executable.
        input : filename, isread, iswrite, isexecute
        output : True
        """
        if not os.path.exists(filename):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % filename)
        if not os.path.isfile(filename):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % filename)
        if isread:
            if not os.access(filename, os.R_OK):
                raise Exception(ErrorCode.GAUSS_501["GAUSS_50100"] % (
                    filename, "the user") +
                                " Error:\n%s: Permission denied." % filename)
        if iswrite:
            if not os.access(filename, os.W_OK):
                raise Exception(ErrorCode.GAUSS_501["GAUSS_50102"] % (
                    filename, "the user") +
                                " Error:\n%s: Permission denied." % filename)
        if isexecute:
            if not os.access(filename, os.X_OK):
                raise Exception(ErrorCode.GAUSS_501[
                                    "GAUSS_50101"] % (filename, "the user") +
                                " Error:\n%s: Permission denied." % filename)
        return True

    def getFileSHA256(self, filename):
        """
        function : Get the ssh file by SHA256
        input : String
        output : String
        """
        if not os.path.exists(filename):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % filename)
        if not os.path.isfile(filename):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % filename)

        strSHA256 = ""
        cmd = g_Platform.getFileSHA256Cmd(filename)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            return strSHA256
        strSHA256 = output.strip()

        return strSHA256

    def getDirSize(self, path, unit=""):
        """
        function : Get the directory or file size
        input : String, String
        output : String
        """
        sizeInfo = ""
        cmd = g_Platform.getDirSizeCmd(path, unit)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            return sizeInfo
        return output.split()[0]

    def getTopPath(self, path):
        """
        function: find the top path of the specified path
        input : NA
        output: tmpDir
        """
        tmpDir = path
        while True:
            # find the top path to be created
            (tmpDir, topDirName) = os.path.split(tmpDir)
            if os.path.exists(tmpDir) or topDirName == "":
                tmpDir = os.path.join(tmpDir, topDirName)
                break
        return tmpDir

    def getFilesType(self, givenPath):
        """
        function : get the file and subdirectory type of the given path
        input : String
        output : String
        """
        if not os.path.exists(givenPath):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % givenPath)
        # obtain the file type
        tmpFile = "/tmp/fileList_%d" % os.getpid()
        cmd = "%s '%s' ! -iname '.*' | %s file -F '::' > %s 2>/dev/null" % (
            g_Platform.getFindCmd(), givenPath,
            g_Platform.getXargsCmd(), tmpFile)
        subprocess.getstatusoutput(cmd)
        # Return code is not equal to zero when file a non-existent
        # file in SLES SP4
        # But it is equal to zero in SLES SP1/SP2/SP3 and
        # RHEL 6.4/6.5/6.6 platform, skip check status and output
        resDict = {}
        try:
            with open(tmpFile, 'r') as fp:
                fileNameTypeList = fp.readlines()
            os.remove(tmpFile)
            for oneItem in fileNameTypeList:
                res = oneItem.split("::")
                if len(res) != 2:
                    continue
                else:
                    resDict[res[0]] = res[1]
            return resDict
        except Exception as e:
            if os.path.exists(tmpFile):
                g_file.removeFile(tmpFile)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50221"] +
                            " Error: \n%s" % str(e))

    # delete a line in file match with re
    def deleteLine(self, filePath, lineInfo):
        """
        function : delete line in a file
        input : filePath ,lineInfo
        output : NA
        """
        cmd = g_Platform.getSedCmd()
        cmd += " -i '/%s/d' %s" % (lineInfo, filePath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % filePath +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def deleteLineByRowNum(self, filePath, lineNum):
        """
        function : delete line in a file by row num
        input : filePath ,lineInfo
        output : NA
        """
        cmd = g_Platform.getSedCmd()
        cmd += " -i '%sd' %s" % (lineNum, filePath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % filePath +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def rename(self, oldFilePath, newFilePath):
        """
        function : rename a file name to new name
        input : oldFilePath, newFilePath
        output : NA
        """
        cmd = g_Platform.getMoveCmd(oldFilePath, newFilePath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50218"] % oldFilePath +
                            " Error:\n%s" % output + "\nThe cmd is %s" % cmd)

    def echoLineToFile(self, line, filePath):
        """
        function : write line in file
        input : line, file
        output : use 2>/dev/null, no return.
        Notice: maye the line has '$'
        """
        cmd = g_Platform.echoCmdWithNoReturn(line, filePath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % filePath +
                            " Command:%s. Error:\n%s" % (cmd, output))

    def checkClusterPath(self, path_name):
        """
        Check the path
        :param path_name:
        :return:
        """
        if not path_name:
            return False

        a_ascii = ord('a')
        z_ascii = ord('z')
        A_ascii = ord('A')
        Z_ascii = ord('Z')
        num0_ascii = ord('0')
        num9_ascii = ord('9')
        blank_ascii = ord(' ')
        sep1_ascii = ord('/')
        sep2_ascii = ord('_')
        sep3_ascii = ord('-')
        sep4_ascii = ord(':')
        sep5_ascii = ord('.')
        sep6_ascii = ord(',')
        for path_char in path_name:
            char_check = ord(path_char)
            if (not (a_ascii <= char_check <= z_ascii or A_ascii <=
                     char_check <= Z_ascii or
                     num0_ascii <= char_check <= num9_ascii or
                     char_check == blank_ascii or
                     char_check == sep1_ascii or
                     char_check == sep2_ascii or
                     char_check == sep3_ascii or
                     char_check == sep4_ascii or
                     char_check == sep5_ascii or
                     char_check == sep6_ascii)):
                return False
        return True

    def checkPathIsLegal(self, path_name):
        """
        check if the path is legal
        :param file name:
        :return:
        """
        if not self.checkClusterPath(path_name):
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51250"] % path_name)

    def cdDirectory(self, dirPath, user=""):
        """
        """
        if (user != "") and (os.getuid() == 0):
            cmd = "su - %s 'cd \'%s\' '" % (user, dirPath)
        else:
            cmd = "cd '%s'" % dirPath
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s " % output)


g_file = fileManage()
