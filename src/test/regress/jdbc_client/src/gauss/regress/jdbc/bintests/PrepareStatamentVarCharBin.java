package gauss.regress.jdbc.bintests;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatamentVarCharBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_varchar(id INT, "
				+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "address varchar(50) ENCRYPTED WITH  (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		List<String> parameters;
		String sqlInsert = "INSERT INTO t_varchar (id, name, address) VALUES (?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("MyName");
		parameters.add("MyAddress");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		conn.fetchData("SELECT * from t_varchar ORDER BY id;");
		
		conn.executeSql("drop table t_varchar;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");	
	}
}
