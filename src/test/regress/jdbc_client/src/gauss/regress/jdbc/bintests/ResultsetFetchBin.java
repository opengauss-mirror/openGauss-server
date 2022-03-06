package gauss.regress.jdbc.bintests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class ResultsetFetchBin implements IBinaryTest {

	/**
	 * This test is for validating the result set cursor - see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
	 * It is specifically meant to test the PgResultset next function in case were CursorResultHandler is invoked
	 * with connection.getQueryExecutor().fetch
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		
		String sql;
		BinUtils.createCLSettings(conn);
		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
			+ "address varchar(50) ENCRYPTED WITH  (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		conn.executeSql(sql);
		//Create records
		for (int i = 0; i < 65; ++i) {
			sql = "INSERT INTO t_varchar (id, name, address) VALUES (" + i + ", 'MyName', 'MyAddress');";
			conn.executeSql(sql);			
		}
		try {
			conn.getConnection().setAutoCommit(false);//Auto commit must be off for this to happen
			Statement st = conn.getConnection().createStatement();
			st.setFetchSize(10);//set the client fetch size to 10
			ResultSet rs = st.executeQuery("select * from t_varchar order by id;");
			conn.printRS4Test(rs);//loop thru the entire data set (should perform initial call & 6 fetch
			st.close();
			conn.getConnection().setAutoCommit(true);//set Auto commit back to on to make the test cleanup
		} catch (SQLException e) {		
			e.printStackTrace();
		}
		
		conn.executeSql("DROP table t_varchar;");		
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		conn.fetchData("select * from gs_column_keys;");
	}

}
