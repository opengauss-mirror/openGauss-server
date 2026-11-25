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

package gauss.regress.jdbc;

// JdbcClientWrapper4LeakTest
public class TestLeak extends JdbcClientWrapper4Test{

	public static void main(String[] args) {
		String serverAddress = args[0];
		String port = args[1];
		String databseName = args[2];
		String username = args[3];
		String password = args[4];
		String testName = args[5];
		execute_test_job(testName, serverAddress, port, databseName, username, password);
		
	}
}
