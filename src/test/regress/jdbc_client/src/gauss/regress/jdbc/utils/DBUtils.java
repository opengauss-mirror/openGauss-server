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

package gauss.regress.jdbc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtils {
	public static Connection connect(String serverAddress, String port,	String databseName, String username, String password) {
		Connection conn = null;
		// currentSchema=public&
		String jdbcConnectionString  = 
				String.format("jdbc:postgresql://%s:%s/%s?enable_ce=1", 
						serverAddress, port, databseName);
		try {
			conn = DriverManager.getConnection(jdbcConnectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void createDatabase4Test(String serverAddress, String port, String databseName, String username, String password, String db2Create) throws SQLException {
		Connection conn = connect(serverAddress, port, "postgres", username, password);
		Statement st = conn.createStatement();
		st.executeUpdate("DROP DATABASE IF EXISTS " + db2Create);
		st.executeUpdate("CREATE DATABASE " + db2Create);
		conn.close();

	}

}

