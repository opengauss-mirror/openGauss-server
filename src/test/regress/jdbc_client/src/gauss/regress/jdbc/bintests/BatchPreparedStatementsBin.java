package gauss.regress.jdbc.bintests;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class BatchPreparedStatementsBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		String sql;
		BinUtils.createCLSettings(conn);
		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
			+ "address varchar(50) ENCRYPTED WITH  (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		conn.executeSql(sql);
		
		PreparedStatement statemnet = null;
		try {
			sql = "INSERT INTO t_varchar (id, name, address) VALUES (?,?,?);";
			conn.getFileWriter().writeLine("starting batch : " + sql);
			statemnet = conn.getConnection().prepareStatement(sql);
			int loopCount = 20;
			conn.getFileWriter().writeLine("Number of rows to add: " + loopCount);
			for (int i = 1; i < loopCount + 1; ++i) {
				statemnet.setInt(1, i);
				statemnet.setString(2, "Name " + i);
				statemnet.setString(3, "Address " + i);
				statemnet.addBatch();
			}
			conn.getFileWriter().writeLine("executing batch ...");
			statemnet.executeBatch();
			
		} catch (SQLException e) {
			conn.getFileWriter().writeLine("Unexpected error:" + e.getMessage());
			e.printStackTrace();
		}
		conn.fetchData("select * from t_varchar order by id;");
		conn.executeSql("DROP table t_varchar;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
	}
}
