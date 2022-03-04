package gauss.regress.jdbc.bintests;
import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatamentStringBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_string(key int,"
				+ "_varchar_ varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_char_ char(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_text_ text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		conn.getFileWriter().writeLine("*******inserting data to the serial table");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "INSERT INTO t_string (key, _varchar_, _char_, _text_) VALUES (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("varchar data");
		parameters.add("char data");
		parameters.add("text data");

		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("varchar data 2");
		parameters.add("char data 2");
		parameters.add("text data 2");

		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		conn.getFileWriter().writeLine("*************inserting data verification");
		String sqlSelect;
		sqlSelect = "select * from t_string where (_varchar_ = ? and _char_ = ?) or _text_ =? order by key";
		parameters = new ArrayList<>();
		parameters.add("varchar data");
		parameters.add("char data");
		parameters.add("text data 2");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("***************updating data");
		String sqlUpdate;
		sqlUpdate = "Update t_string set _varchar_ =? , _char_ = ? where _text_ = ? or key = ?";
		parameters = new ArrayList<>();
		parameters.add("varchar updated data");
		parameters.add("char updated data");
		parameters.add("text data 2");
		parameters.add("2");
		conn.updateDataWithPrepareStmnt(sqlUpdate, parameters);

		conn.getFileWriter().writeLine("**************updating data verification");
		sqlSelect = "select * from t_string where _varchar_ = ? and _char_ = ?";
		parameters = new ArrayList<>();
		parameters.add("varchar updated data");
		parameters.add("char updated data");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		conn.getFileWriter().writeLine("*************deleting data");
		String sqlDelete;
		sqlDelete = "delete from  t_string where _varchar_ = ? and _text_ = ? and _char_ = ? ";
		parameters = new ArrayList<>();
		parameters.add("varchar updated data");
		parameters.add("text data 2");
		parameters.add("char updated data");

		conn.updateDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("*******************deleting data verification");
		sqlSelect = "select * from t_string where _varchar_ = ? and _text_ = ? and _char_ = ?";
		parameters = new ArrayList<>();
		parameters.add("varchar updated data");
		parameters.add("text data 2");
		parameters.add("char updated data");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		sqlSelect = "select * from t_string";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("*************deleting all data");
		sqlDelete = "delete  from t_string";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("**************deleting all data verification");
		sqlSelect = "select * from t_string";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

				
		conn.executeSql("drop table t_string;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		
	}
}
