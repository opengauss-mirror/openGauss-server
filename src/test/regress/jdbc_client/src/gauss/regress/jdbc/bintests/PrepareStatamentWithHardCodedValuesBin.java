package gauss.regress.jdbc.bintests;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatamentWithHardCodedValuesBin implements IBinaryTest {

	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_varchar(id INT, "
				+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "address varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		List<String> parameters;
		String sqlInsert = "INSERT INTO t_varchar (id, name, address) VALUES (?,?,?);";
		
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("MyName");
		parameters.add("MyAddress");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("MyName2");
		parameters.add("MyAddress2");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);

		sqlInsert = "INSERT INTO t_varchar (id, name, address) VALUES (?, 'MyName3',?);";
		parameters = new ArrayList<>();
		parameters.add("3");
		parameters.add("MyAddress3");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		conn.fetchData("SELECT * from t_varchar ORDER BY id;");
		
		parameters = new ArrayList<>();
		parameters.add("MyName");
		conn.fetchDataWithPrepareStmnt("select * from t_varchar where name = ? and address = 'MyAddress';", parameters);
		
		conn.fetchData("SELECT * from t_varchar ORDER BY id;");
		conn.executeSql("drop table t_varchar;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");

		
	}

}
