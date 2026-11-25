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

REM src/tools/msvc/pgflex.bat
REM all the logic for this now belongs in pgflex.pl. This file really
REM only exists so you don't have to type "perl src/tools/msvc/pgflex.pl"
REM Resist any temptation to add any logic here.
@perl src/tools/msvc/pgflex.pl %*
