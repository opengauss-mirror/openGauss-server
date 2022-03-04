package gauss.regress.jdbc.bintests;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class FunctionsReturnValuesBin implements IBinaryTest {
	
	/**
	 * This test is for testing the callable statement with function return values  
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);

		
		//Add a table without client logic if any test need to be compared with "regular" results 
		conn.executeSql("CREATE TABLE IF NOT EXISTS t_num_non_cl(id INT, num int)");
		conn.executeSql("INSERT INTO t_num_non_cl (id, num) VALUES (1, 5555)");
		conn.executeSql("INSERT INTO t_num_non_cl (id, num) VALUES (2, 6666)");
		conn.fetchData("SELECT * from t_num_non_cl order by id");
	
		
		conn.executeSql("CREATE TABLE IF NOT EXISTS t_num(id INT, num int ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC))");
		conn.executeSql("INSERT INTO t_num (id, num) VALUES (1, 5555)");
		conn.executeSql("INSERT INTO t_num (id, num) VALUES (2, 6666)");
		conn.fetchData("SELECT * from t_num order by id");
				
		conn.executeSql("CREATE FUNCTION reffunc(refcursor) RETURNS refcursor AS "
				+ "'BEGIN OPEN $1 FOR SELECT * FROM t_num; "
    			+ " RETURN $1; "
    			+ "END; "
    			+ "' LANGUAGE plpgsql;");
    			
		conn.executeSql("CREATE FUNCTION f_processed_return_table() RETURNS TABLE(val_p int, val2_p int) \n"
			+ "as \n"
			+ "$BODY$ \n"
			+ "begin \n"
			+ "return query (SELECT id, num from t_num); \n"
			+ "end; \n"
			+ "$BODY$ \n"
			+  "language plpgsql ;\n");


		conn.executeSql("CREATE FUNCTION select1 () RETURNS t_num LANGUAGE SQL AS 'SELECT * from t_num;'");
		conn.getFileWriter().writeLine("Invoking select1 using simple query:");
		conn.fetchData("call select1();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select1", 2);

		conn.executeSql("CREATE FUNCTION select2 () RETURNS t_num LANGUAGE SQL AS 'SELECT id, num from t_num;';");
		conn.getFileWriter().writeLine("Invoking select2 using simple query:");
		conn.fetchData("call select2();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select2", 2);

		conn.executeSql("CREATE FUNCTION select3 () RETURNS setof t_num LANGUAGE SQL AS 'SELECT * from t_num;'");
		conn.getFileWriter().writeLine("Invoking select3 using simple query:");
		conn.fetchData("call select3();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select3", 2);
		//TODO: can only get the values of the first record. 
		// I was expecting that we should be able to get one output parameter with the type of Types.REF_CURSOR
		// But it did not work, throw an error:
		// org.postgresql.util.PSQLException: A CallableStatement was executed with an invalid number of parameters
		// https://stackoverflow.com/questions/44982250/why-cant-postgres-functions-that-return-setof-be-called-from-a-jdbc-callablesta
		// Seems not supported - see this https://jdbc.postgresql.org/documentation/81/callproc.html
		//Maybe this bug in the postgres JDBC driver is related: https://github.com/pgjdbc/pgjdbc/issues/633
		//Code is below
//		try {
//			
//			conn.getFileWriter().writeLine("Invoking select3 using CallableStatement:");
//			CallableStatement callStmnt = conn.getConnection().prepareCall("{call select3(?)}");
//			callStmnt.registerOutParameter(1, Types.REF_CURSOR);
//			callStmnt.execute();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			conn.getFileWriter().writeLine("ERROR running select3 " + e.getMessage());
//		}
		conn.executeSql("CREATE FUNCTION select4 () RETURNS setof t_num LANGUAGE SQL AS 'SELECT id, num from t_num;'");
		conn.fetchData("call select4();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select4", 2);
		
		conn.executeSql("CREATE FUNCTION select5 () RETURNS int LANGUAGE SQL AS 'SELECT num from t_num;'");
		conn.getFileWriter().writeLine("Invoking select5 using simple query:");
		conn.fetchData("call select5();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select5", 1);
		
		conn.executeSql("CREATE FUNCTION select6 () RETURNS setof int LANGUAGE SQL AS 'SELECT  num from t_num;';");
		conn.fetchData("call select6();");
		BinUtils.invokeFunctionWithIntegerOutParams(conn, "select6", 1);
		//As above cannot use REF_CURSOR to get the entire set of records as it is expecting an integer parameter 
		
		conn.executeSql("CREATE FUNCTION select7 () RETURNS TABLE(a INT, b INT) LANGUAGE SQL AS "
				+ "'SELECT id, num from t_num;';");
		conn.fetchData("call select7();");
		//As above cannot use REF_CURSOR to get the entire set of records as it is expecting an integer parameter
		
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
		conn.fetchData("call get_rows_setof()");
		conn.fetchData("CALL f_processed_return_table();");
		//As above cannot use REF_CURSOR to get the entire set of records as it is expecting an integer parameter
		//conn.fetchData("BEGIN;);
		//SELECT reffunc('funccursor');
		//FETCH ALL IN funccursor;
		//COMMIT;
		//SELECT * FROM get_rows_setof();
		//
		conn.executeSql("DROP FUNCTION select1;");
		conn.executeSql("DROP FUNCTION select2;");
		conn.executeSql("DROP FUNCTION select3;");
		conn.executeSql("DROP FUNCTION select4;");
		conn.executeSql("DROP FUNCTION select5;");
		conn.executeSql("DROP FUNCTION select6;");
		conn.executeSql("DROP FUNCTION select7;");
		conn.executeSql("DROP FUNCTION reffunc(refcursor);");
		conn.executeSql("DROP FUNCTION get_rows_setof();");
		conn.executeSql("DROP FUNCTION f_processed_return_table();");
		conn.executeSql("DROP TABLE t_num CASCADE;");
		conn.fetchData("SELECT COUNT(*) FROM gs_encrypted_proc;");
		conn.fetchData("SELECT proname, prorettype, proallargtypes "
				+ "FROM gs_encrypted_proc JOIN pg_proc ON pg_proc.Oid = gs_encrypted_proc.func_id;");
		
		BinUtils.dropCLSettings(conn);
		
	}
}
