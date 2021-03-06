rem #######################################################################
rem Copyright (c): 2020-2025, Huawei Tech. Co., Ltd.
rem descript: Compile windows ODBC
rem           Return 0 means OK.
rem           Return error-code means failed.
rem version:  1.0
rem date:     2020-12-29
rem #######################################################################

@echo off
rem Make sure current dir is .\Build\Script

rem error number
set ERRORNO=0
set ERROR_VCVAR_NOTEXIST=1
set ERROR_VCVAR_RUN=2
set ERROR_BUILD_LIBPQ=3
set ERROR_BUILD_PGSQL=4
set ERROR_BUILD_ODBC=5
set ERROR_CREATE_MSI=6
set ERROR_WINRAR_NOTEXIST=7
set ERROR_FULLBRANCH_NULL=8

set ROOT_DIR=%~dp0
set BASE_DIR=%ROOT_DIR%..\..
set ODBC_TMP_DIR=%ROOT_DIR%odbc_tmp

set FULLBRANCH=%1
if "%FULLBRANCH%" == "" (
    echo FULLBRANCH is null
    set ERRORNO=%ERROR_FULLBRANCH_NULL% && goto END
)

set TAR_FILE=%ROOT_DIR%%FULLBRANCH%-Windows-Odbc.zip

rem Clean up old files and folders
@echo Clean up old files and folders
if exist %TAR_FILE% (
	@echo clear old %FULLBRANCH%-Odbc.zip
	del %TAR_FILE%
)

if exist %ODBC_TMP_DIR% (
	@echo clear odbc tmp dir
	rd /s/q %ODBC_TMP_DIR%
)

rem Create temporary folders
@echo Create temporary folders
md %ODBC_TMP_DIR%

call:BuildOdbc x64 Release
if %ERRORLEVEL% NEQ 0 set ERRORNO=%ERRORLEVEL% && goto END
call:BuildOdbc Win32 Release
if %ERRORLEVEL% NEQ 0 set ERRORNO=%ERRORLEVEL% && goto END

@echo ========================================================================
@echo ==========================create zip file===============================
if exist C:\"Program Files (x86)"\WinRAR (
	set winrar=C:\"Program Files (x86)"\WinRAR\winrar
) else if exist C:\"Program Files"\WinRAR (
	set winrar=C:\"Program Files"\WinRAR\winrar
) else (
	echo WinRAR was nowhere to be found in C:\"Program Files (x86)"\WinRAR or C:\"Program Files"\WinRAR
	set ERRORNO=%ERROR_WINRAR_NOTEXIST% && goto END
)

cd %ODBC_TMP_DIR%
%winrar% a -r -ibck %TAR_FILE% .\*
cd %ROOT_DIR%

:END
rem delete tmp dir
if exist %ODBC_TMP_DIR% (
	@echo clear tmp dir
	rd /s/q %ODBC_TMP_DIR%
)

if %ERRORNO% NEQ 0 (
	echo %~n0.bat failed to execute and the return value was %ERRORNO%.
	echo %ERROR_VCVAR_NOTEXIST%:ERROR_VCVAR_NOTEXIST
	echo %ERROR_VCVAR_RUN%:ERROR_VCVAR_RUN
	echo %ERROR_BUILD_LIBPQ%:ERROR_BUILD_LIBPQ
	echo %ERROR_BUILD_PGSQL%:ERROR_BUILD_PGSQL
	echo %ERROR_BUILD_ODBC%:ERROR_BUILD_ODBC
	echo %ERROR_CREATE_MSI%:ERROR_CREATE_MSI
	echo %ERROR_WINRAR_NOTEXIST%:ERROR_WINRAR_NOTEXIST
	echo %ERROR_FULLBRANCH_NULL%:ERROR_FULLBRANCH_NULL
) else (
	echo %~n0.bat was executed successfully and the generated file was "%TAR_FILE%".
)
exit /B %ERRORNO%


rem function BuildeOdbc
rem param platform  Win32\x64
rem param build Release\Debug
:BuildOdbc
set PLATFORM=%~1
set BUILD=%~2

rem build param
set SOLUTION_CONFIG="%BUILD%|%PLATFORM%"
set ACTION=Rebuild

@echo ========================================================================
@echo =========================run vcvarsall.bat==============================
set vcvars="%VS100COMNTOOLS%\..\..\VC\vcvarsall.bat"
@echo %vcvars%
if not exist %vcvars% (
	@echo vcvars does not exist
	exit /B %ERROR_VCVAR_NOTEXIST%
)

if "%PLATFORM%"=="x64"	call %vcvars% x86_amd64
if "%PLATFORM%"=="Win32" call %vcvars% x86
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_VCVAR_RUN%

rem create libpq project
@echo ========================================================================
@echo ====================create libpq.sln psqlodbc.sln=======================
set LIPQ_BUILD_PATH=%BASE_DIR%\src\tools\msvc
cd %LIPQ_BUILD_PATH%
call build.bat libpq
cd %ROOT_DIR%
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_BUILD_LIBPQ%

rem build libpq project
@echo ========================================================================
@echo =========================build libpq.sln================================
set LIBPQ_SOLUTION=%BASE_DIR%\pgsql.sln
devenv %LIBPQ_SOLUTION% /%ACTION% %SOLUTION_CONFIG% /Project libpq 
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_BUILD_PGSQL%

rem build psqlodbc project
@echo ========================================================================
@echo ========================build psqlodbc.sln==============================
set ODBC_SOLUTION=%BASE_DIR%\openGauss-connector-odbc\psqlodbc.sln
devenv %ODBC_SOLUTION% /%ACTION% %SOLUTION_CONFIG%
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_BUILD_ODBC%

rem use WiX to create an installer for the application
@echo ========================================================================
@echo ==========use WiX to create an installer for the application============
set INSTALLER_DIR=%BASE_DIR%\openGauss-connector-odbc\installer
rem copy %BASE_DIR%\binarylibs\win2003_x86\openssl\comm\bin\*.dll %INSTALLER_DIR%\x86
rem copy %BASE_DIR%\binarylibs\win2003_x86_64\openssl\comm\bin\*.dll %INSTALLER_DIR%\x64

cd %BASE_DIR%\openGauss-connector-odbc\installer
if "%PLATFORM%"=="x64"	call MakeX64.bat
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_CREATE_MSI%
if "%PLATFORM%"=="Win32" call Make.bat
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_CREATE_MSI%
cd %ROOT_DIR%
if %ERRORLEVEL% NEQ 0 exit /B %ERROR_CREATE_MSI%

rem copy file to .\Build\Script
@echo ========================================================================
@echo ==========copy file to %ODBC_TMP_DIR%============
if "%PLATFORM%"=="x64" (
	md %ODBC_TMP_DIR%\x64
	@echo copy Release\libpq\libpq.dll
	copy %BASE_DIR%\Release\libpq\libpq.dll %ODBC_TMP_DIR%\x64
	
	@echo copy openGauss-connector-odbc\installer\x64\psqlodbc_x64.msi
	copy %BASE_DIR%\openGauss-connector-odbc\installer\x64\psqlodbc_x64.msi %ODBC_TMP_DIR%
	
	@echo copy psqlodbc35w.dll msvcr100.dll msvcr100d.dll openssl.dll
	copy %BASE_DIR%\openGauss-connector-odbc\Release-x64\psqlodbc35w.dll %ODBC_TMP_DIR%\x64
	copy %BASE_DIR%\openGauss-connector-odbc\installer\x64\msvcr100.dll %ODBC_TMP_DIR%\x64
	copy %BASE_DIR%\openGauss-connector-odbc\installer\x64\msvcr100d.dll %ODBC_TMP_DIR%\x64
	rem copy %BASE_DIR%\binarylibs\win2003_x86_64\openssl\comm\bin\*.dll %ODBC_TMP_DIR%\x64
) else if "%PLATFORM%"=="Win32" (
	md %ODBC_TMP_DIR%\x86
	@echo copy Release\libpq\libpq.dll
	copy %BASE_DIR%\Release\libpq\libpq.dll %ODBC_TMP_DIR%\x86
	
	@echo copy openGauss-connector-odbc\installer\psqlodbc.msi
	copy %BASE_DIR%\openGauss-connector-odbc\installer\psqlodbc.msi %ODBC_TMP_DIR%
	
	@echo copy psqlodbc35w.dll msvcr100.dll msvcr100d.dll
	copy %BASE_DIR%\openGauss-connector-odbc\Release-x86\psqlodbc35w.dll %ODBC_TMP_DIR%\x86
	copy %BASE_DIR%\openGauss-connector-odbc\installer\x86\msvcr100.dll %ODBC_TMP_DIR%\x86
	copy %BASE_DIR%\openGauss-connector-odbc\installer\x86\msvcr100d.dll %ODBC_TMP_DIR%\x86
	rem copy %BASE_DIR%\binarylibs\win2003_x86\openssl\comm\bin\*.dll %ODBC_TMP_DIR%\x86
)

GOTO:EOF
