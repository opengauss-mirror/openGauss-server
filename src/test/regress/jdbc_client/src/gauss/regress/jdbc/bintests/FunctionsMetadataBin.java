package gauss.regress.jdbc.bintests;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class FunctionsMetadataBin implements IBinaryTest {
	/**
	 * This test checks that the metedata service returns information of the original type
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		//Run the test 3 times: 
		// 1. Without client logic objects
		createScenario(conn, false);
		getFunctionMetadata(conn);
		removeScenario(conn);
		//
		// 2. With client logic objects
		BinUtils.createCLSettings(conn);
		createScenario(conn, true);
		getFunctionMetadata(conn);
		removeScenario(conn);
		BinUtils.dropCLSettings(conn);
		//
		// 3. with connection that has no client logic - to make sure we did not affect the regular queris
		conn.connectWithNoCL();
		createScenario(conn, false);
		getFunctionMetadata(conn);
		removeScenario(conn);
	}
	/**
	 * Gets the function metadata from the DatabaseMetaData service
	 * @param conn
	 * @param metadataService
	 */
	private void getFunctionMetadata(DatabaseConnection4Test conn) {
		DatabaseMetaData metadataService = null;
		try {
			metadataService = conn.getConnection().getMetaData();
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Failed obtaining the DatabaseMetaData service, existing test ...");
			e.printStackTrace();
			return;
		}

		try {
			conn.getFileWriter().writeLine("Obtaining the list of columns");
			ResultSet columns = metadataService.getFunctionColumns(null, "public", null, null);
			Set<String> ignoreColumn = new HashSet<>();
			ignoreColumn.add("SPECIFIC_NAME");
			conn.printRS4Test(columns, ignoreColumn);
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("metadataService failed , error:" + e.getMessage());
			e.printStackTrace();
		}
	}
	/**
	 * Create the scenario with with out client logic for comparison
	 * @param conn
	 * @param withCl
	 */
	void createScenario(DatabaseConnection4Test conn, boolean withCl) {
		
		final String clientLogicFragment = "ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC)";
		
		List<String> tables = Arrays.asList(new String[]{
				"CREATE TABLE t_processed (name varchar(100) CLIENT_LOGIC, "
				+ "id INT CLIENT_LOGIC, val INT CLIENT_LOGIC, val2 INT)",
				"CREATE TABLE t_num(id INT, num int CLIENT_LOGIC)"
				});
		
		for (String table : tables) {
			if (withCl) {
				conn.executeSql(table.replaceAll("CLIENT_LOGIC", clientLogicFragment));
			}
			else {
				conn.executeSql(table.replaceAll("CLIENT_LOGIC", ""));
			}
		}
		
		conn.executeSql("CREATE OR REPLACE FUNCTION "
				+ "t_processed(in1 int, in2 int, in3 int, out1 OUT int, out2 OUT varchar, out3 OUT int) "
				+ "AS 'SELECT val, name, val2 from t_processed "
				+ "where id = in1 and id = in2 and val2 = in3 LIMIT 1' "
				+ "LANGUAGE SQL");

		conn.executeSql("CREATE OR REPLACE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "$$ BEGIN SELECT val, val2 from t_processed ORDER BY name LIMIT 1 INTO out1, out2; END;"
				+ "$$ LANGUAGE PLPGSQL");

		
		conn.executeSql("CREATE FUNCTION select4 () RETURNS setof t_num LANGUAGE SQL AS 'SELECT id, num from t_num;'");
		
		conn.executeSql("CREATE FUNCTION select5 () RETURNS int LANGUAGE SQL AS 'SELECT num from t_num;'");
		conn.getFileWriter().writeLine("Invoking select5 using simple query:");
		
		conn.executeSql("CREATE FUNCTION select6 () RETURNS setof int LANGUAGE SQL AS 'SELECT  num from t_num;';");
		
		conn.executeSql("CREATE FUNCTION select7 () RETURNS TABLE(a INT, b INT) LANGUAGE SQL AS "
				+ "'SELECT id, num from t_num;';");
		
		conn.executeSql("CREATE OR REPLACE FUNCTION get_rows_setof() RETURNS SETOF t_num AS \n" 
				+ "$BODY$ \n"
				+ "DECLARE \n"
				+ "r t_num%rowtype; \n"
				+ "BEGIN \n"
				+ "FOR r IN \n"
				+ "SELECT * FROM t_num \n"
				+ "LOOP \n" 
				+ "-- can do some processing here \n"
				+ "RETURN NEXT r; -- return current row of SELECT \n"
				+ "END LOOP; \n"
				+ "RETURN; \n"
				+ "END \n"
				+ "$BODY$ \n"
				+ "LANGUAGE plpgsql;");		
	}
	/**
	 * Removes the database objects created by the test
	 * @param conn
	 */
	void removeScenario(DatabaseConnection4Test conn) {
		conn.executeSql("DROP FUNCTION t_processed");
		conn.executeSql("DROP FUNCTION f_plaintext_out");
		conn.executeSql("DROP FUNCTION select4");
		conn.executeSql("DROP FUNCTION select5");
		conn.executeSql("DROP FUNCTION select6");
		conn.executeSql("DROP FUNCTION select7");
		conn.executeSql("DROP FUNCTION get_rows_setof");
		conn.executeSql("DROP TABLE t_processed");
		conn.executeSql("DROP TABLE t_num");
	}
}
