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
/* End of automatic include section */
#define ECPGdebug(X,Y) ECPGdebug((X)+100,(Y))

#line 1 "comment.pgc"
#include <stdlib.h>


#line 1 "regression.h"
#ifndef REGRESSION_H
#define REGRESSION_H







#endif /* REGRESSION_H */

#line 3 "comment.pgc"


/* just a test comment */ int i;
/* just a test comment int j*/;

/****************************************************************************/
/* Test comment                                                             */
/*--------------------------------------------------------------------------*/

int main(void)
{
  ECPGdebug(1, stderr);

  { ECPGconnect(__LINE__, 0, "regress1" , NULL, NULL , NULL, 0); }
#line 17 "comment.pgc"


  { ECPGdisconnect(__LINE__, "CURRENT");}
#line 19 "comment.pgc"

  exit (0);
}
