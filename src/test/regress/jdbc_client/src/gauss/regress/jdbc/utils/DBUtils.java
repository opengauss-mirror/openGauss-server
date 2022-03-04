package gauss.regress.jdbc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtils {
	public static Connection connect(String serverAddress, String port,	String databseName, String username, String password) {
		Connection conn = null;
		// currentSchema=public&
		String jdbcConnectionString  = 
				String.format("jdbc:postgresql://%s:%s/%s?enable_ce=1", 
						serverAddress, port, databseName);
		try {
			conn = DriverManager.getConnection(jdbcConnectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void createDatabase4Test(String serverAddress, String port, String databseName, String username, String password, String db2Create) throws SQLException {
		Connection conn = connect(serverAddress, port, "postgres", username, password);
		Statement st = conn.createStatement();
		st.executeUpdate("DROP DATABASE IF EXISTS " + db2Create);
		st.executeUpdate("CREATE DATABASE " + db2Create);
		conn.close();

	}

}

