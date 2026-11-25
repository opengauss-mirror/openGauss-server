REM Copyright (c) 2026 Huawei Technologies Co., Ltd.
REM openGauss-server is licensed under Mulan PSL v2.
REM You can use this software according to the terms and conditions of the Mulan PSL v2.
REM You may obtain a copy of Mulan PSL v2 at:
REM          http://license.coscl.org.cn/MulanPSL2
REM THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
REM EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
REM MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
REM See the Mulan PSL v2 for more details.
REM Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
REM Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved

@echo off
:: This bootstrap scripts set up the build environment for TimescaleDB
:: Any flags will be passed on to CMake, e.g.,
:: ./bootstrap.bat -DCMAKE_BUILD_TYPE="Debug"


:: Get source directory to build from
set ORIG=%0
for %%F in (%ORIG%) do set SRC_DIR=%%~dpF

SET BUILD_DIR=./build

IF EXIST "%BUILD_DIR%" (
	setlocal EnableDelayedExpansion
	ECHO Build system already initialized in %BUILD_DIR%
	SET /P resp="Do you want to remove it (this is IMMEDIATE and PERMANENT), y/n? "
	IF "!resp!" == "y" (
		rd /s /q "%BUILD_DIR%"
	) ELSE (
		ECHO Exiting
		EXIT
	)
)

mkdir "%BUILD_DIR%"
cd "%BUILD_DIR%"
cmake %SRC_DIR% -A x64 %*

ECHO ---
ECHO TimescaleDB build system initialized in %BUILD_DIR%.
ECHO To compile, do:
ECHO     cmake --build %BUILD_DIR% --config Release
