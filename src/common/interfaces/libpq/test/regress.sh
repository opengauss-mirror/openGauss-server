#!/bin/bash
# ***********************************************************************
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
# Generate regress diff report
# version: 1.0.0
# change log:
# ***********************************************************************

set -e

while read line
do
	echo "trying $line"
	./uri-regress "$line"
	echo ""
done < "${SRCDIR}/${SUBDIR}"/regress.in >regress.out 2>&1

if diff -c "${SRCDIR}/${SUBDIR}/"expected.out regress.out >regress.diff; then
	echo "========================================"
	echo "All tests passed"
	exit 0
else
	echo "========================================"
	echo "FAILED: the test result differs from the expected output"
	echo
	echo "Review the difference in ${SUBDIR}/regress.diff"
	echo "========================================"
	exit 1
fi
