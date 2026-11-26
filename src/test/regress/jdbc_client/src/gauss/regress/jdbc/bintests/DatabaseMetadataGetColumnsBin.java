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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;
import gauss.regress.jdbc.utils.MetadtaUtils;

/**
 * Test the Database metadata API on table Run it 3 times: On table with client
 * logic column On table with no client logic On table with no client logic with
 * no CL in the connection string
 */
public class DatabaseMetadataGetColumnsBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		MetadtaUtils.create_client_logic_table(conn);
		MetadtaUtils.create_simple_table(conn);
		try {
			conn.getFileWriter().writeLine("Testing table with client logic ...");
			String query = "Select * from metadata_client_logic_test_tbl;";
			String tableName = "metadata_client_logic_test_tbl";
			runTest(conn, query, tableName);
			MetadtaUtils.drop_client_logic_tbl_data(conn);
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("Testing table with no client logic ...");
			query = "Select * from metadata_simple_test_tbl;";
			tableName = "metadata_simple_test_tbl";
			runTest(conn, query, tableName);
			conn.getFileWriter()
					.writeLine("Testing table with no client logic and with no client logic in connection string ...");
			conn.connectWithNoCL();
			runTest(conn, query, tableName);
		} catch (Exception e) {
			conn.getFileWriter().writeLine("Failed ResutsetMetadata");
			e.printStackTrace();
		}
		MetadtaUtils.drop_simple_tbl_data(conn);
	}

	/**
	 * Encapsulate the test into a function to run it twice
	 * 
	 * @param conn      JDBC connection
	 * @param query     SQL to use
	 * @param tableName use for table properties
	 * @throws SQLException
	 */

	private void runTest(DatabaseConnection4Test conn, String query, String tableName) throws SQLException {
		DatabaseMetaData metaData = conn.getConnection().getMetaData();
		ResultSet columns = metaData.getColumns(null, null, tableName, null);
		while (columns.next()) {
			conn.getFileWriter().writeLine("Column name: " + columns.getString("COLUMN_NAME"));
			conn.getFileWriter().writeLine("Column size" + "(" + columns.getInt("COLUMN_SIZE") + ")");
			conn.getFileWriter().writeLine("Ordinal position: " + columns.getInt("ORDINAL_POSITION"));
			conn.getFileWriter().writeLine("Catalog: " + columns.getString("TABLE_CAT"));
			conn.getFileWriter().writeLine("Data type (integer value): " + columns.getInt("DATA_TYPE"));
			conn.getFileWriter().writeLine("Data type name: " + columns.getString("TYPE_NAME"));
			conn.getFileWriter().writeLine("Decimal value: " + columns.getBigDecimal("DECIMAL_DIGITS"));
			conn.getFileWriter().writeLine(" ");
			conn.getFileWriter().writeLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			conn.getFileWriter().writeLine(" ");
		}
	}
}
