package gauss.regress.jdbc.bintests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class DisconnectedRsBin implements IBinaryTest {

	@Override
	public void execute(DatabaseConnection4Test conn) {
		String sql;

		BinUtils.createCLSettings(conn);
		
		sql = "CREATE TABLE IF NOT EXISTS t_varchar_regular(id INT, name varchar(50), address varchar(50));";
		conn.executeSql(sql);

		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
			+ "address varchar(50) ENCRYPTED WITH  (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		conn.executeSql(sql);

		sql = "INSERT INTO t_varchar (id, name, address) VALUES (1, 'MyName', 'MyAddress');";
		conn.executeSql(sql);
		sql = "INSERT INTO t_varchar VALUES (2, 'MyName2', 'MyAddress2');";
		conn.executeSql(sql);

		sql = "INSERT INTO t_varchar_regular (id, name, address) VALUES (1, 'MyName', 'MyAddress');";	
		conn.executeSql(sql);
		sql = "INSERT INTO t_varchar_regular VALUES (2, 'MyName2', 'MyAddress2');";
		conn.executeSql(sql);
		try {
			Statement st = conn.getConnection().createStatement();
			sql = "SELECT * from t_varchar_regular ORDER BY id;";
			ResultSet rs = st.executeQuery(sql);
			conn.executeSql("drop table t_varchar;");
			conn.executeSql("drop table t_varchar_regular;");
			conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
			conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
			conn.getConnection().close();
			conn.getFileWriter().writeLine("Connection is closed");
			conn.printRS4Test(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

}
