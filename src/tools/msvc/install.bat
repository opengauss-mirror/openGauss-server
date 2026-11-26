REM Copyright (c) 2026 Huawei Technologies Co., Ltd.
REM openGauss-server is licensed under Mulan PSL v2.
REM You can use this software according to the terms and conditions of the Mulan PSL v2.
REM You may obtain a copy of Mulan PSL v2 at:
REM          http://license.coscl.org.cn/MulanPSL2
REM THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
REM EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
REM MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
REM See the Mulan PSL v2 for more details.
REM Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group

@echo off
REM src/tools/msvc/install.bat

if NOT "%1"=="" GOTO RUN_INSTALL

echo Invalid command line options.
echo Usage: "install.bat <path>"
echo.
REM exit fix for pre-2003 shell especially if used on buildfarm
if "%XP_EXIT_FIX%" == "yes" exit 1
exit /b 1

:RUN_INSTALL

SETLOCAL

IF NOT EXIST buildenv.pl goto nobuildenv
perl -e "require 'buildenv.pl'; while(($k,$v) = each %%ENV) { print qq[\@SET $k=$v\n]; }" > bldenv.bat
CALL bldenv.bat
del bldenv.bat
:nobuildenv

perl install.pl "%1"

REM exit fix for pre-2003 shell especially if used on buildfarm
if "%XP_EXIT_FIX%" == "yes" exit %ERRORLEVEL%
exit /b %ERRORLEVEL%
