package gauss.regress.jdbc.bintests;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class CreateFunctionBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		String sql;
		sql = "CREATE TABLE sbtest1(id int," +
				"k INTEGER ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC)," +
				"c CHAR(120) ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC)," +
				"pad CHAR(60) ENCRYPTED WITH (column_encryption_key = cek1, encryption_type = DETERMINISTIC));";
		
		conn.executeSql(sql);
		sql = "insert into sbtest1 values (1,1,1,1)";
		
		conn.executeSql(sql);
		sql = "create function select_data() "
				+ "returns table(a int, b INTEGER, c CHAR(120), d CHAR(120)) " + 
				"as " + 
				"$BODY$ " +
				"begin " +
				"return query(select * from sbtest1); " +
				"end; " +
				"$BODY$ " +
				"LANGUAGE plpgsql; ";
		conn.executeSql(sql);
		
		sql = "call select_data(); ";
		conn.fetchData(sql);
		sql = "DROP FUNCTION select_data";
		conn.executeSql(sql);
		sql = "DROP TABLE sbtest1;";
		conn.executeSql(sql);
		
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1");
	}

}
