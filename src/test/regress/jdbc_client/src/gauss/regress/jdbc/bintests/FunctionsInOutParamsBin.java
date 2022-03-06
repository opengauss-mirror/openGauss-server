package gauss.regress.jdbc.bintests;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class FunctionsInOutParamsBin implements IBinaryTest {
	/**
	 * This test checks function with an input and out parameters that are client logic
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		
		BinUtils.createCLSettings(conn);

		conn.executeSql("CREATE TABLE t_processed "
				+ "(name varchar(100) ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), "
				+ "id INT ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), "
				+ "val INT ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), val2 INT)");

		conn.executeSql("insert into t_processed values"
				+ "('one',1,10,10),('two',2,20,20),('three',3,30,30),('four',4,40,40),('five',5,50,50),('six',6,60,60),('seven',7,70,70),"
				+ "('eight',8,80,80),('nine',9,90,90),('ten',10,100,100)");
		
		conn.fetchData("SELECT * from t_processed order by val2");

		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_in_out_1_int_param(in1 int, out1 OUT int) "
		+ "AS 'SELECT val from t_processed  where id = in1 LIMIT 1' LANGUAGE SQL");

		conn.fetchData("SELECT f_processed_in_out_1_int_param(2)");
		try {
			conn.getFileWriter().writeLine("Invoking f_processed_in_out_1_int_param using CallableStatement");
			CallableStatement stmnt = conn.getConnection().prepareCall("{? = call f_processed_in_out_1_int_param(?)}");
			stmnt.setInt(1, 2);
			stmnt.registerOutParameter(2, Types.INTEGER);
			stmnt.execute();
			Object data = stmnt.getObject(2);
			BinUtils.printParameter(conn, data, "f_processed_in_out_1param", 2);
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Error invoking f_processed_in_out_1param :" + e.getMessage());
			e.printStackTrace();
		}

		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_in_int_out_varchar(in1 int, out1 OUT varchar) "
		+ "AS 'SELECT name from t_processed  where id = in1 LIMIT 1' LANGUAGE SQL");
		conn.fetchData("SELECT f_processed_in_int_out_varchar(2)");
		try {
			conn.getFileWriter().writeLine("Invoking f_processed_in_int_out_varchar using CallableStatement");
			CallableStatement stmnt = conn.getConnection().prepareCall("{? = call f_processed_in_int_out_varchar(?)}");
			stmnt.setInt(1, 2);
			stmnt.registerOutParameter(2, Types.VARCHAR);
			stmnt.execute();		
			Object data = stmnt.getObject(2);
			BinUtils.printParameter(conn, data, "f_processed_in_out_1param_varchar_out", 2);
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Error invoking f_processed_in_out_1param_varchar :" + e.getMessage());
			e.printStackTrace();
		}
		
		conn.executeSql("CREATE OR REPLACE FUNCTION f_processed_varchar_in_int_out(in1 varchar, out1 OUT int) "
		+ "AS 'SELECT id from t_processed  where name = in1 LIMIT 1' LANGUAGE SQL");
		conn.fetchData("SELECT f_processed_varchar_in_int_out('one')");
		try {
			conn.getFileWriter().writeLine("Invoking f_processed_varchar_in_int_out using CallableStatement");
			CallableStatement stmnt = conn.getConnection().prepareCall("{? = call f_processed_varchar_in_int_out(?)}");
			stmnt.setString(1, "one");
			stmnt.registerOutParameter(2, Types.INTEGER);
			stmnt.execute();
			Object data = stmnt.getObject(2);
			BinUtils.printParameter(conn, data, "f_processed_varchar_in_int_out", 2);
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Error invoking f_processed_varchar_in_int_out :" + e.getMessage());
			e.printStackTrace();
		}
		conn.executeSql("DROP FUNCTION f_processed_in_out_1_int_param");
		conn.executeSql("DROP FUNCTION f_processed_in_int_out_varchar");
		conn.executeSql("DROP FUNCTION f_processed_varchar_in_int_out");
		conn.executeSql("DROP TABLE t_processed CASCADE");
		BinUtils.dropCLSettings(conn);
	}
}
