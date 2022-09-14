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
# make_solaris_libpq.sh
#    Build libpq in solaris platform
#
# IDENTIFICATION
#    src/common/interfaces/libpq/make_solaris_libpq.sh
#
#-------------------------------------------------------------------------
set -e

LIBPQ_PATH="$(pwd)/${LIBPQ_PATH}"
declare ROOT_DIR="${LIBPQ_PATH}../../../../"

# create pg_config_paths.h for make
touch pg_config_paths.h
echo "#define PGBINDIR \" \"" >> pg_config_paths.h
echo "#define PGSHAREDIR \" \"" >> pg_config_paths.h
echo "#define SYSCONFDIR \" \"" >> pg_config_paths.h
echo "#define INCLUDEDIR \" \"" >> pg_config_paths.h
echo "#define PKGINCLUDEDIR \" \"" >> pg_config_paths.h
echo "#define INCLUDEDIRSERVER \" \"" >> pg_config_paths.h
echo "#define LIBDIR \" \"" >> pg_config_paths.h
echo "#define PKGLIBDIR \" \"" >> pg_config_paths.h
echo "#define LOCALEDIR \" \"" >> pg_config_paths.h
echo "#define DOCDIR \" \"" >> pg_config_paths.h
echo "#define HTMLDIR \" \"" >> pg_config_paths.h
echo "#define MANDIR \" \"" >> pg_config_paths.h

# create errcodes.h for make
touch ../../include/utils/errcodes.h

# ln gs_strerror.cpp for make
ln -s ../../../src/port/gs_strerror.cpp gs_strerror.cpp

# make all files for libpq
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-auth.o fe-auth.cpp
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/include/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-connect.o fe-connect.cpp
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/include/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-exec.o fe-exec.cpp
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -DPC_LINT -c -o fe-misc.o fe-misc.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-print.o fe-print.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-lobj.o fe-lobj.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-protocol2.o fe-protocol2.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-protocol3.o fe-protocol3.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o pqexpbuffer.o pqexpbuffer.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o pqsignal.o pqsignal.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o fe-secure.o fe-secure.cpp 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o libpq-events.o libpq-events.cpp 
rm -f chklocale.cpp && ln -s ../../../src/port/chklocale.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o chklocale.o chklocale.cpp
rm -f inet_net_ntop.cpp && ln -s ../../../src/port/inet_net_ntop.cpp . 
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o inet_net_ntop.o inet_net_ntop.cpp 
rm -f noblock.cpp && ln -s ../../../src/port/noblock.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o noblock.o noblock.cpp 
rm -f pgstrcasecmp.cpp && ln -s ../../../src/port/pgstrcasecmp.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o pgstrcasecmp.o pgstrcasecmp.cpp
rm -f snprintf.cpp && ln -s ../../../src/port/snprintf.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o snprintf.o snprintf.cpp
rm -f thread.cpp && ln -s ../../../src/port/thread.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o thread.o thread.cpp
rm -f cipher.cpp && ln -s ../../../src/port/cipher.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o cipher.o cipher.cpp
rm -f path.cpp && ln -s ../../../src/port/path.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o path.o path.cpp
rm -f pg_crc.cpp && ln -s ../../../src/port/pg_crc.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o pg_crc.o pg_crc.cpp
rm -f strlcpy.cpp && ln -s ../../../src/port/strlcpy.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o strlcpy.o strlcpy.cpp 
rm -f getpeereid.cpp && ln -s ../../../src/port/getpeereid.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o getpeereid.o getpeereid.cpp
rm -f ip.cpp && ln -s ../../../src/backend/libpq/ip.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o ip.o ip.cpp 
rm -f md5.cpp && ln -s ../../../src/backend/libpq/md5.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o md5.o md5.cpp
rm -f sha2.cpp && ln -s ../../../src/backend/libpq/sha2.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o  -c -o sha2.o sha2.cpp
rm -f encnames.cpp && ln -s ../../../src/backend/utils/mb/encnames.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o encnames.o encnames.cpp
rm -f wchar.cpp && ln -s ../../../src/backend/utils/mb/wchar.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o wchar.o wchar.cpp
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o gs_strerror.o gs_strerror.cpp
rm -f gs_env_r.cpp && ln -s ../../../src/backend/../port/gs_env_r.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o gs_env_r.o gs_env_r.cpp
rm -f gs_syscall_lock.cpp && ln -s ../../../src/backend/../port/gs_syscall_lock.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o gs_syscall_lock.o gs_syscall_lock.cpp
rm -f gs_readdir.cpp && ln -s ../../../src/backend/../port/gs_readdir.cpp .
g++ -m64 -DSTREAMPLAN -DPGXC -O0 -Wall -Wpointer-arith -Wno-write-strings -fnon-call-exceptions -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -g -pthreads -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -D_REENTRANT -D_THREAD_SAFE -D_POSIX_PTHREAD_SEMANTICS -fpic -DFRONTEND -DUNSAFE_STAT_OK -I. -I../../../src/include -I../../../src/port -I../../../src/port -D_GNU_SOURCE -DSO_MAJOR_VERSION=5 -DPC_LINT -c -o gs_readdir.o gs_readdir.cpp
g++ -fpic -Wall -mimpure-text -s -DNDEBUG -O2 -m64 -shared -o libpq.so fe-auth.o snprintf.o fe-connect.o fe-exec.o fe-misc.o fe-print.o fe-lobj.o fe-protocol2.o fe-protocol3.o pqexpbuffer.o pqsignal.o fe-secure.o libpq-events.o chklocale.o inet_net_ntop.o noblock.o pgstrcasecmp.o thread.o cipher.o path.o pg_crc.o strlcpy.o getpeereid.o ip.o md5.o sha2.o encnames.o wchar.o gs_strerror.o gs_env_r.o gs_syscall_lock.o gs_readdir.o -L${ROOT_DIR}/Platform/solaris_10_64/Huawei_Secure_C -L${ROOT_DIR}/Platform/solaris_10_64/ssl -lcrypt -lpthread -lsocket -lnsl -lresolv -lsecurec -lc -lipsi_ssl -lipsi_pse -lipsi_crypto -lipsi_osal

# clear the file created for make 
rm ../../include/utils/errcodes.h
rm gs_strerror.cpp
rm pg_config_paths.h
