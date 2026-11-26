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

package gauss.regress.jdbc.bintests;

import java.sql.SQLException;
import java.sql.Statement;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class BatchStatamentBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		String sql;
		BinUtils.createCLSettings(conn);
		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
			+ "address varchar(50) ENCRYPTED WITH  (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		conn.executeSql(sql);
		
		try {
			Statement statemnet = conn.getConnection().createStatement();
			int loopCount = 20;
			for (int i = 1; i < loopCount + 1; ++i) {
				sql = "INSERT INTO t_varchar (id, name, address) VALUES (" + i + ", 'MyName" + i + "', 'MyAddress" + i + "');";
				conn.getFileWriter().writeLine("added to batch " + sql);
				statemnet.addBatch(sql);
			}
			conn.getFileWriter().writeLine("executing batch ...");
			statemnet.executeBatch();
			conn.fetchData("select * from t_varchar order by id;");
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Unexpected error:" + e.getMessage());
			e.printStackTrace();
		}
	
		conn.executeSql("DROP table t_varchar;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
	}
}
