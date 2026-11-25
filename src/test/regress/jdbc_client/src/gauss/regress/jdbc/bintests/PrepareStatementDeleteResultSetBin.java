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
import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSet;
import java.sql.Statement;
import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatementDeleteResultSetBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn){
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_delete_rows_tbl(key int PRIMARY KEY ,"
				+ "col_varchar varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "col_int int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "col_float float ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		conn.getFileWriter().writeLine("*******inserting data to the t_delete_rows_tbl");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "INSERT INTO t_delete_rows_tbl (key, col_varchar, col_int, col_float) VALUES (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("this_row_will_be_deleted");
		parameters.add("1");
		parameters.add("1.1");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("this_row_will_not_deleted");
		parameters.add("2");
		parameters.add("2.2");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		String sqlSelect;
		conn.getFileWriter().writeLine("*************verify data before the delete");
		sqlSelect = "select * from t_delete_rows_tbl order by key;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
	
		try {
			Statement stmt= conn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			sqlSelect = "select * from t_delete_rows_tbl;";
		    ResultSet rs = stmt.executeQuery(sqlSelect);
		    while(rs.next()) {
		         if(rs.getInt("key")==1) {
		        	rs.deleteRow();
		    
		         }     
		    }
		}
	    catch (Exception e) {
	            System.out.println("Failed to delete result row\n"+ e);
	    }
		conn.getFileWriter().writeLine("*************verifying the deleted data");
		sqlSelect = "select * from t_delete_rows_tbl;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
	
		conn.executeSql("drop table t_delete_rows_tbl;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		
	}		
}
		
