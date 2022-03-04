package gauss.regress.jdbc.bintests;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class ClientLogicCacheRetryBin implements IBinaryTest{

	/**
	 * This test it added to validate the client logic cache retry mechanism of the JDBC driver
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		
		// Create CL column settings
		createCLColumnSettings(conn);
		createCLSettingsAndTable(conn);
		// batch and cache retry is not yet supported createCLSettingsAndInsertBatch2Table(conn);
		// Create and update Data while refreshing the cache: 
		createCLSettingsAndInsert2Table(conn);
		createCLSettingsAndInsert2TableUsingPrepare(conn);
		createCLSettingsAndUpdating2Table(conn);
		
		// Read data while performing cache refresh
		createCLSettingsAndReadwithCriteria(conn, true);
		createCLSettingsAndReadwithCriteria(conn, false);			
		//  multiple sql is not supported createTableAndReadFrom2ndConnectionMultiRS(conn);
		createTableAndReadFrom2ndConnection(conn);

		// Function with CL settings
		checkFunctionCacheRefresh(conn);
	}
	/**
	 * Writes header to the output file, to make it easy to distinguish between test cases
	 * @param conn
	 * @param header
	 */
	private void writHeader(DatabaseConnection4Test conn, String header) {
		conn.getFileWriter().writeLine("");
		for (int i = 0; i  < header.length() + 4; ++i) {
			conn.getFileWriter().write("*");
		}
		conn.getFileWriter().writeLine("");
		conn.getFileWriter().write("* ");
		conn.getFileWriter().write(header);
		conn.getFileWriter().write(" *");
		conn.getFileWriter().writeLine("");
		
		for (int i = 0; i  < header.length() + 4; ++i) {
			conn.getFileWriter().write("*");
		}
		conn.getFileWriter().writeLine("");
	}
	/**
	 * Validate the cache refresh mechanism as per function definitions with client logic parameters
	 * @param conn
	 */
	private void checkFunctionCacheRefresh(DatabaseConnection4Test conn) {
					
		writHeader(conn, "Cache retry for functions with client logic parameters");

		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
		
		DatabaseConnection4Test conn2 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn2.connectWithoutReloadingCacheOnIsValid();
		
		BinUtils.createCLSettings(conn0);
		
		conn0.executeSql("CREATE TABLE t_processed "
				+ "(name text, val INT ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC), val2 INT)");
		conn0.executeSql("insert into t_processed "
				+ "values('one',1,10),('two',2,20),('three',3,30),('four',4,40),('five',5,50),('six',6,60),"
				+ "('seven',7,70),('eight',8,80),('nine',9,90),('ten',10,100)");
		
		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		/* Use case - return record ID
		 * This use case is not yet covered as we must access the server to resolve the actual data type
		 */
		conn0.executeSql("CREATE  FUNCTION f_out_only(out1 OUT int, out2 OUT int) AS "
				+ "'SELECT val, val2 from t_processed ORDER BY name LIMIT 1' LANGUAGE SQL");
		conn0.fetchData("SELECT f_out_only ()");			
		conn1.fetchData("SELECT f_out_only ()");

		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.getFileWriter().writeLine("Trying SELECT f_out_only () again after calling to isValid method:");
		conn1.fetchData("SELECT f_out_only()");
		conn2.getFileWriter().writeLine("conn2, which is to be used now have refreshClientEncryption set to zero");
		conn2.fetchData("SELECT f_out_only()");
		try {
			if (!conn2.getConnection().isValid(60)) {
				conn2.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn2.getFileWriter().writeLine("conn2 isValid Failed with error");
			e.printStackTrace();
		}
		conn2.fetchData("SELECT f_out_only()");

		/* Use case - In out parameters
		 * This is use case handled by capturing the error of function not defined since on the server it is defined with client logic and not integer
		 */
		conn0.executeSql("CREATE FUNCTION f_plaintext_out(out1 INOUT int, out2 INOUT int) AS "
				+ "'SELECT val, val2 from t_processed where val=out1 AND val2=out2 ORDER BY name LIMIT 1' LANGUAGE SQL");
		conn0.fetchData("CALL f_plaintext_out (3, 30)");
		conn0.fetchData("SELECT f_plaintext_out (3, 30)");

		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.fetchData("SELECT f_plaintext_out (3, 30)");
		
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 5555)");
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (2, 6666)");
		conn0.fetchData("SELECT * from t_num");
		
		/* Use case - function return value
		 * This is use case handled on the resultset level when it cannot extract user value from client logic value
		 */
		conn0.executeSql("CREATE FUNCTION select1 () RETURNS t_num LANGUAGE SQL AS 'SELECT * from t_num;';");
		conn0.fetchData("call select1()");
		conn1.fetchData("call select1()");
		
		/* Use case - function return value
		 * This is use case handled on the resultset level when it cannot extract user value from client logic value
		 */
		conn0.executeSql("CREATE FUNCTION f_processed_in_out_plpgsql(in1 int, out out1 int, in2 int, out out2 int)"
				+ "as $$ "
				+ "begin "
				+ "select val, val2 INTO out1, out2 from t_processed where val = in2 or val = in1 limit 1; "
				+ "end;$$ "
				+ "LANGUAGE plpgsql");
		conn0.fetchData("SELECT f_processed_in_out_plpgsql(17,3)");

		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.fetchData("SELECT f_processed_in_out_plpgsql(17,3)");
		
		conn0.executeSql("DROP function f_out_only");
		conn0.executeSql("DROP function f_plaintext_out;");
		conn0.executeSql("DROP function select1");
		conn0.executeSql("DROP function f_processed_in_out_plpgsql");
		conn0.executeSql("DROP TABLE t_num CASCADE");
		conn0.executeSql("DROP TABLE t_processed CASCADE");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();
		conn2.close();
	}
	/**
	 * Validate the cache refresh when trying to read client logic data and change it to user format on a 2nd connection 
	 * @param conn
	 */
	private void createTableAndReadFrom2ndConnection(DatabaseConnection4Test conn) {

		writHeader(conn, "Validate the cache refresh when trying to read data");
		/* conn0 is used to create the CL settings */
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/* conn0 is used as a 2nd connection that has the cache not updated */
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
		
		BinUtils.createCLSettings(conn0);			
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 555)");
		conn0.fetchData("select * from t_num");

		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.fetchData("select * from t_num");
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}
	/**
	 * Validate the cache refresh when trying to read data - multiple resultsets
	 * @param conn
	 */
	private void createTableAndReadFrom2ndConnectionMultiRS(DatabaseConnection4Test conn) {
		writHeader(conn, "Validate the cache refresh when trying to read data - multiple resultsets");

		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
		
		BinUtils.createCLSettings(conn0);
					
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 555)");
		/* did not support mmultiple sql */
		String sql = "select * from t_num;select num from t_num;";
		BinUtils.getMultipleResultsets(conn0, sql);
		BinUtils.getMultipleResultsets(conn1, sql);
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}
	/**
	 * Validate the cache retry mechanism when inserting data to a table using prepare statement
	 * @param conn
	 */
	private void createCLSettingsAndInsert2TableUsingPrepare(DatabaseConnection4Test conn) {
		writHeader(conn, "Validate the cache retry mechanism when inserting data to a table using prepare statement");

		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();

		BinUtils.createCLSettings(conn0);			
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		String sql = "INSERT INTO t_num (id, num) VALUES (?,?)";
		List<String> parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("2");
		conn1.updateDataWithPrepareStmnt(sql, parameters);
		
		conn1.fetchData("select * from t_num");
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();						
	}
	/**
	 * Validate the cache retry mechanism when trying to select with a where clause pointing to a field with client logic
	 * @param conn 
	 * @param usePrepare if to use prepare statement
	 */
	private void createCLSettingsAndReadwithCriteria(DatabaseConnection4Test conn, boolean usePrepare) {
		String header = "Validate cache retry when applying where clause on client logic field";
		if (usePrepare) {
			header += " using prepare statament";	
		}
		else {
			header += " Using simple queries";
		}
		writHeader(conn, header);
		
		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn1 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();

		BinUtils.createCLSettings(conn0);			
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");

		String sql = "INSERT INTO t_num (id, num) VALUES (?,?)";
		List<String> parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("2");
		conn0.updateDataWithPrepareStmnt(sql, parameters);
		conn0.fetchData("select * from t_num");

		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		
		if (usePrepare) {
			sql = "SELECT * FROM t_num where num = ?";
			List<String> parameters2 = new ArrayList<>();
			parameters2.add("2");
			conn1.fetchDataWithPrepareStmnt(sql, parameters2);
		}
		else {
			conn1.fetchData("SELECT * FROM t_num where num = 2");
		}
		conn1.fetchData("select * from t_num");
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();						
	}

	/**
	 * Validate cache retry when updating data using simple query
	 * @param conn
	 */
	private void createCLSettingsAndUpdating2Table(DatabaseConnection4Test conn) {
		this.writHeader(conn, "Validate cache retry when updating data using simple query");

		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();

		BinUtils.createCLSettings(conn0);			
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 555)");
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 666)");
		conn0.fetchData("select * from t_num");
		
		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.executeSql("update t_num set num = 7000");
		conn1.fetchData("select * from t_num");
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}		
	/**
	 * Validate cache retry when inserting data using simple query
	 * @param conn
	 */
	private void createCLSettingsAndInsert2Table(DatabaseConnection4Test conn) {
		this.writHeader(conn, "Validate cache retry when inserting data using simple query");

		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();

		/*conn1 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();

		BinUtils.createCLSettings(conn0);			
		conn0.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		conn0.executeSql("INSERT INTO t_num (id, num) VALUES (1, 555)");
		conn0.fetchData("select * from t_num");
		
		try {
			if (!conn1.getConnection().isValid(60)) {
				conn1.getFileWriter().writeLine("isValid Failed for connection 1");
			}
		} catch (SQLException e) {
			conn1.getFileWriter().writeLine("isValid Failed with error");
			e.printStackTrace();
		}
		conn1.executeSql("INSERT INTO t_num (id, num) VALUES (1, 666)");
		conn1.fetchData("select * from t_num");
		
		conn0.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}
	/**
	 * Validate cache retry when inserting data using batch query
	 * @param conn
	 */
	private void createCLSettingsAndInsertBatch2Table(DatabaseConnection4Test conn) {
		this.writHeader(conn, "Validate cache retry when inserting data in batch");
		//
		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
		//			
		String sql;
		BinUtils.createCLSettings(conn0);
		sql = "CREATE TABLE t_varchar(id INT, "
				+ "name varchar(50) ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC),"
				+ "address varchar(50) ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC));";
		conn0.executeSql(sql);		
		
		try {
			Statement statemnet = conn1.getConnection().createStatement();
			int loopCount = 20;
			for (int i = 1; i < loopCount + 1; ++i) {
				sql = "INSERT INTO t_varchar (id, name, address) "
						+ "VALUES (" + i + ", 'MyName" + i + "', 'MyAddress" + i + "')";
				conn1.getFileWriter().writeLine("added to batch " + sql);
				statemnet.addBatch(sql);
			}
			conn1.getFileWriter().writeLine("executing batch ...");
			statemnet.executeBatch();
			conn1.fetchData("select * from t_varchar order by id");
		} catch (SQLException e) {
			e.printStackTrace();
			conn1.getFileWriter().writeLine("Unexpected error:" + e.getMessage());
		}
	
		conn0.executeSql("DROP table t_varchar");
		BinUtils.dropCLSettings(conn0);

		conn0.close();
		conn1.close();			
	}

	/**
	 * Validate cache retry when creating a column settings
	 * @param conn
	 */
	private void createCLColumnSettings(DatabaseConnection4Test conn) {
		this.writHeader(conn, "Validate cache retry when creating a column settings");
		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
        conn1.gsKtoolExec("\\! gs_ktool -d all");
		conn1.gsKtoolExec("\\! gs_ktool -g");
		String sql;
		sql = "CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);";
		conn1.executeSql(sql);
		sql = "CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);";
        conn1.executeSql(sql);
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}
	
	/**
	 * Validate cache retry when using 2nd connection to create a table
	 * @param conn
	 */
	private void createCLSettingsAndTable(DatabaseConnection4Test conn) {
		this.writHeader(conn, "Validate cache retry when using 2nd connection to create a table");
		//
		/*conn0 is used to create the CL settings*/
		DatabaseConnection4Test conn0 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn0.reconnect();
		/*conn0 is used as a 2nd connection that has the cache not updated*/
		DatabaseConnection4Test conn1 = new DatabaseConnection4Test(conn, conn.getFileWriter());
		conn1.reconnect();
		//			
		BinUtils.createCLSettings(conn0);			
		conn1.executeSql("CREATE TABLE t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		
		conn1.executeSql("DROP TABLE t_num");
		BinUtils.dropCLSettings(conn0);
		
		conn0.close();
		conn1.close();			
	}
}
