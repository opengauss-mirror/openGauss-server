#!/bin/bash
#
# mkldexport
#	create an AIX exports file from an object file
#
# src/backend/port/aix/mkldexport.sh
#
# Usage:
#	mkldexport objectfile [location]
# where
#	objectfile is the current location of the object file.
#	location is the eventual (installed) location of the
#		object file (if different from the current
#		working directory).
#
# [This file comes from the Postgres 4.2 distribution. - ay 7/95]
#
# Header: /usr/local/devel/postgres/src/tools/mkldexport/RCS/mkldexport.sh,v 1.2 1994/03/13 04:59:12 aoki Exp
#

# setting this to nm -B might be better
# ... due to changes in AIX 4.x ...
# ... let us search in different directories - Gerhard Reithofer
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# SPDX-License-Identifier: PostgreSQL
#
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
if [ -x /usr/ucb/nm ]
then NM=/usr/ucb/nm
elif [ -x /usr/bin/nm ]
then NM=/usr/bin/nm
elif [ -x /usr/ccs/bin/nm ]
then NM=/usr/ccs/bin/nm
elif [ -x /usr/usg/bin/nm ]
then NM=/usr/usg/bin/nm
else echo "Fatal error: cannot find 'nm' ... please check your installation."
     exit 1
fi

CMDNAME=$(basename $0)
if [ -z "$1" ]; then
	echo "Usage: $CMDNAME object [location]"
	exit 1
fi
OBJNAME=$(basename $1)
if [ "$(basename $OBJNAME)" != "$(basename $OBJNAME .o)" ]; then
	OBJNAME=$(basename $OBJNAME .o).so
fi
if [ -z "$2" ]; then
	echo '#!'
else
	if [ "$2" = "." ]; then
		# for the base executable (AIX 4.2 and up)
		echo '#! .'
	else
		echo '#!' $2
	fi
fi
$NM -BCg $1 | \
	egrep ' [TDB] ' | \
	sed -e 's/.* //' | \
	egrep -v '\$' | \
	sed -e 's/^[.]//' | \
	sort | \
	uniq
