package gauss.regress.jdbc.bintests;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class FunctionsInoutParamBin implements IBinaryTest {
	/**
	 * This test checks function with an inout parameters that are client logic using the callable statement interface
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		
		BinUtils.createCLSettings(conn);

		conn.executeSql("CREATE TABLE t_processed "
				+ "(name text, val INT ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), val2 INT)");
		conn.executeSql("insert into t_processed "
				+ "values('one',1,10),('two',2,20),('three',3,30),('four',4,40),('five',5,50),('six',6,60),"
				+ "('seven',7,70),('eight',8,80),('nine',9,90),('ten',10,100)");
		
		conn.executeSql("CREATE OR REPLACE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "'SELECT val, val2 from t_processed where val= out1 ORDER BY name LIMIT 1' LANGUAGE SQL");
		conn.fetchData("CALL f_plaintext_out (1, 1)");
		executeWithCallableStatament(conn, 1, 1);
		conn.executeSql("DROP FUNCTION f_plaintext_out");
		//
		conn.executeSql("CREATE OR REPLACE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "'SELECT val, val2 from t_processed where val=out1 AND val2=out2 ORDER BY name LIMIT 1' LANGUAGE SQL");
		conn.fetchData("CALL f_plaintext_out (3, 30)");
		conn.fetchData("SELECT f_plaintext_out (3, 30)");
		executeWithCallableStatament(conn, 3, 30);
		//
		conn.executeSql("DROP FUNCTION f_plaintext_out");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "$$ BEGIN SELECT val, val2 from t_processed ORDER BY name LIMIT 1 INTO out1, out2; END;"
				+ "$$ LANGUAGE PLPGSQL");
		conn.fetchData("CALL f_plaintext_out (2, 3)");
		conn.fetchData("SELECT f_plaintext_out (2, 3)");
		executeWithCallableStatament(conn, 2, 30);
		//
		conn.executeSql("DROP FUNCTION f_plaintext_out");
		conn.executeSql("CREATE OR REPLACE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "$$ BEGIN SELECT val, val2 from t_processed where val=out1 or val2=out2 "
				+ "ORDER BY name LIMIT 1 INTO out1, out2; END; $$ LANGUAGE PLPGSQL");
		conn.fetchData("CALL f_plaintext_out (2, 30)");
		conn.fetchData("SELECT f_plaintext_out (2, 30)");
		executeWithCallableStatament(conn, 2, 30);
		
		conn.executeSql("DROP FUNCTION f_plaintext_out");
		conn.executeSql("DROP TABLE t_processed");
		BinUtils.dropCLSettings(conn);
	}
	/**
	 * Method to execute the inout functions defined in this test
	 * @param conn
	 * @param p1
	 * @param p2
	 */
	private void executeWithCallableStatament(DatabaseConnection4Test conn, int p1, int p2) {
		try {
			CallableStatement callableStatement = conn.getConnection().prepareCall("{CALL f_plaintext_out (?, ?)}");
			callableStatement.registerOutParameter(1, Types.INTEGER);
			callableStatement.registerOutParameter(2, Types.INTEGER);
			callableStatement.setInt(1, p1);
			callableStatement.setInt(2, p2);
			callableStatement.execute();
			Object data1 = callableStatement.getObject(1);
			BinUtils.printParameter(conn, data1, "f_plaintext_out", 1);
			Object data2 = callableStatement.getObject(2);
			BinUtils.printParameter(conn, data2, "f_plaintext_out", 2);
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("executeWithCallableStatament failed, error:" + e.getMessage());
			e.printStackTrace();
		}

	}
}

