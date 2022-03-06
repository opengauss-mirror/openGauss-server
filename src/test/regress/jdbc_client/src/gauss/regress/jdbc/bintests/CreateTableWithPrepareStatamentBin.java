package gauss.regress.jdbc.bintests;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class CreateTableWithPrepareStatamentBin  implements IBinaryTest{
	/**
	 * This test is to validate the prepare statement flow
	 */
	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);

		List<String> parameters;
		parameters = new ArrayList<>();
		String sqlCreate = "CREATE TABLE IF NOT EXISTS "
				+ "t_string(key int,"
				+ "_varchar_ varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_char_ char(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_text_ text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
		

		conn.executeSql(sqlCreate);
		
		String sqlInsert;
		sqlInsert = "INSERT INTO t_string (key, _varchar_, _char_, _text_) VALUES (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("varchar data");
		parameters.add("char data");
		parameters.add("text data");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		String sqlInsertSimple = "INSERT INTO t_string (key, _varchar_, _char_, _text_) "
				+ "VALUES (1,'2','3','4');";
		conn.executeSql(sqlInsertSimple);
		
		conn.executeSql("INSERT INTO t_string (key, _varchar_, _char_, _text_) "
				+ "VALUES (1,'2','3','4');");
		conn.fetchData("select * from t_string;");
		
		conn.executeSql("DROP TABLE t_string;");
		BinUtils.dropCLSettings(conn);
	}
}
