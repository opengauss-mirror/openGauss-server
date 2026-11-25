/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* Processed by ecpg (regression mode) */
/* These include files are added by the preprocessor */
#include <ecpglib.h>
#include <ecpgerrno.h>
#include <sqlca.h>
/* Needed for informix compatibility */
#include <ecpg_informix.h>
/* End of automatic include section */
#define ECPGdebug(X,Y) ECPGdebug((X)+100,(Y))

#line 1 "charfuncs.pgc"
#include <stdio.h>
#include <stdlib.h>
#include <sqltypes.h>

int main(void)
{
	char t1[] = "abc  def  ghi  ";
	          /* 123456789012345 */
	char buf[50];
	int k;

	printf("t1: _%s_\n", t1);
	rupshift(t1);
	printf("t1: _%s_\n", t1);

	k = 2;
	ldchar(t1, k, buf);
	printf("byleng(t1, %d): %d, ldchar: _%s_\n", k, byleng(t1, k), buf);
	k = 5;
	ldchar(t1, k, buf);
	printf("byleng(t1, %d): %d, ldchar: _%s_\n", k, byleng(t1, k), buf);
	k = 9;
	ldchar(t1, k, buf);
	printf("byleng(t1, %d): %d, ldchar: _%s_\n", k, byleng(t1, k), buf);
	k = 15;
	ldchar(t1, k, buf);
	printf("byleng(t1, %d): %d, ldchar: _%s_\n", k, byleng(t1, k), buf);


	return 0;
}
