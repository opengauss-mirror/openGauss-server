package gauss.regress.jdbc.bintests;
import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatamentIntBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_int(key int,"
				+ "_smallint_ smallint ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_int_ int ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_bigint_ bigint ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		conn.getFileWriter().writeLine("*******inserting data to the int table");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "INSERT INTO t_int (key, _smallint_, _int_, _bigint_) VALUES (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("-3333");
		parameters.add("0");
		parameters.add("3333");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("-1234");
		parameters.add("256");
		parameters.add("1234");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		conn.getFileWriter().writeLine("*************inserting data verification");
		String sqlSelect;
		sqlSelect = "select * from t_int where (_smallint_ = ? and _int_ =?) or _bigint_ =? order by key";
		parameters = new ArrayList<>();
		parameters.add("-3333");
		parameters.add("0");
		parameters.add("1234");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("***************updating data");
		String sqlUpdate;
		sqlUpdate = "Update t_int set _smallint_= ? , _int_= ? where  _bigint_ = ? or key = ?";
		parameters = new ArrayList<>();
		parameters.add("5555");
		parameters.add("5555");
		parameters.add("1234");
		parameters.add("2");
		conn.updateDataWithPrepareStmnt(sqlUpdate, parameters);

		conn.getFileWriter().writeLine("**************updating data verification");
		sqlSelect = "select * from t_int where _smallint_ = ? and _int_ =?";
		parameters = new ArrayList<>();
		parameters.add("5555");
		parameters.add("5555");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		conn.getFileWriter().writeLine("*************deleting data");
		String sqlDelete;
		sqlDelete = "delete from  t_int where _smallint_= ? and _bigint_= ? and _int_ = ?";
		parameters = new ArrayList<>();
		parameters.add("5555");
		parameters.add("1234");
		parameters.add("5555");
		conn.updateDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("*******************deleting data verification");
		sqlSelect = "select * from t_int where _smallint_= ? and _bigint_= ? and _int_ = ?";
		parameters = new ArrayList<>();
		parameters.add("5555");
		parameters.add("1234");
		parameters.add("5555");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		sqlSelect = "select * from t_int;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("*************deleting all data");
		sqlDelete = "delete  from t_int";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("**************deleting all data verification");
		sqlSelect = "select * from t_int;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

				
		conn.executeSql("drop table t_int;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		
	}
}
