package gauss.regress.jdbc.bintests;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class BatchSimpleQueryBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		String sql;
		
		BinUtils.createCLSettings(conn);
		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
			+ "address varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		conn.executeSql(sql);
		StringBuilder sqlBuilder = new StringBuilder();
		int loopCount = 2;
		for (int i = 1; i < loopCount + 1; ++i) {
			sqlBuilder.append("INSERT INTO t_varchar (id, name, address) "
					+ "VALUES (" + i + ", 'MyName-" + i + "', 'MyAddress-" + i + "');");
		}
		conn.executeSql(sqlBuilder.toString());

		// split mutiple statements to adapt to current features
		for (int i = 1; i < loopCount + 1; ++i) {
			sql = "INSERT INTO t_varchar (id, name, address) "
					+ "VALUES (" + i + ", 'MyName-" + i + "', 'MyAddress-" + i + "');";
			conn.executeSql(sql);
		}

		conn.fetchData("SELECT * from t_varchar ORDER BY id;");		
		conn.executeSql("DROP table t_varchar;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
	}
}
