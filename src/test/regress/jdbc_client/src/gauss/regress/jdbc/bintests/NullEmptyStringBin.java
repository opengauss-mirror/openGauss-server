package gauss.regress.jdbc.bintests;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

/**
 * Testing for client logic fields wih null, empty string and regular values 
 * @author Avi Kessel
  */
public class NullEmptyStringBin implements IBinaryTest {

	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);

		conn.executeSql("CREATE TABLE IF NOT EXISTS t_not_cl(id INT, name varchar(50));");
		String sqlWith2Params = "INSERT INTO t_not_cl (id, name) VALUES (?, ?);";
		String sqlWithNoParams = "INSERT INTO t_not_cl (id) VALUES (2);"; 
		
		List<String> parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("MyName");
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		
		conn.executeSql(sqlWithNoParams);
				
		parameters = new ArrayList<>();
		parameters.add("3");
		parameters.add("");
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		parameters = new ArrayList<>();
		parameters.add("4");
		parameters.add(null);
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		
		conn.fetchData("select id is null, name is null from t_not_cl order by id;");
		conn.fetchData("select * from t_not_cl order by id;");
		
		conn.executeSql("CREATE TABLE IF NOT EXISTS t_with_cl(id INT, "
			+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		sqlWith2Params = "INSERT INTO t_with_cl (id, name) VALUES (?, ?);";
		sqlWithNoParams = "INSERT INTO t_with_cl (id) VALUES (2);"; 
		
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("MyName");
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		
		conn.executeSql(sqlWithNoParams);
				
		parameters = new ArrayList<>();
		parameters.add("3");
		parameters.add("");
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		parameters = new ArrayList<>();
		parameters.add("4");
		parameters.add(null);
		conn.updateDataWithPrepareStmnt(sqlWith2Params, parameters);
		
		conn.fetchData("select id is null, name is null from t_with_cl order by id;");
		conn.fetchData("select * from t_with_cl order by id;");

		conn.executeSql("DROP TABLE t_not_cl;");
		conn.executeSql("DROP TABLE t_with_cl;");
		BinUtils.dropCLSettings(conn);
	}
}
