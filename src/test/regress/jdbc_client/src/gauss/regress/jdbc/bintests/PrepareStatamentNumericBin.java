package gauss.regress.jdbc.bintests;
import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatamentNumericBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_numeric(key int,"
				+ "_real_ real ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_decimal_ decimal ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_doubleprecision_ double precision ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "_numeric_ numeric ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		conn.getFileWriter().writeLine("*******inserting data to the serial table");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "INSERT INTO t_numeric (key, _real_, _decimal_, _numeric_, _doubleprecision_) VALUES (?,?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("1234.1234");
		parameters.add("5678.5678");
		parameters.add("91011.91011");
		parameters.add("12131415.12131415");

		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("111213.111213");
		parameters.add("141516.141516");
		parameters.add("17181920.17181920");
		parameters.add("2122232425.2122232425");

		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		conn.getFileWriter().writeLine("*************inserting data verification");
		String sqlSelect;
		sqlSelect = "select * from t_numeric where (_real_ = ? and _decimal_ =?) or (_numeric_ =?  and _doubleprecision_= ?) order by key;";
		parameters = new ArrayList<>();
		parameters.add("1234.1234");
		parameters.add("5678.5678");
		parameters.add("17181920.17181920");
		parameters.add("2122232425.2122232425");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("***************updating data");
		String sqlUpdate;
		sqlUpdate = "Update t_numeric set _real_= ? , _decimal_= ? where  _numeric_ = ? or key = ?";
		parameters = new ArrayList<>();
		parameters.add("212223.212223");
		parameters.add("24252627.24252627");
		parameters.add("17181920.17181920");
		parameters.add("2");
		conn.updateDataWithPrepareStmnt(sqlUpdate, parameters);

		conn.getFileWriter().writeLine("**************updating data verification");
		sqlSelect = "select * from t_numeric where _real_ = ? and _decimal_ =?";
		parameters = new ArrayList<>();
		parameters.add("212223.212223");
		parameters.add("24252627.24252627");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		conn.getFileWriter().writeLine("*************deleting data");
		String sqlDelete;
		sqlDelete = "delete from  t_numeric where _real_= ? and _numeric_= ? and _decimal_ = ? and  _doubleprecision_ =?";
		parameters = new ArrayList<>();
		parameters.add("212223.212223");
		parameters.add("17181920.17181920");
		parameters.add("24252627.24252627");
		parameters.add("2122232425.2122232425");

		conn.updateDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("*******************deleting data verification");
		sqlSelect = "select * from t_numeric where _real_= ? and _numeric_= ? and _decimal_ = ? and  _doubleprecision_ =?";
		parameters = new ArrayList<>();
		parameters.add("212223.212223");
		parameters.add("17181920.17181920");
		parameters.add("24252627.24252627");
		parameters.add("2122232425.2122232425");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

		sqlSelect = "select * from t_numeric;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.getFileWriter().writeLine("*************deleting all data");
		sqlDelete = "delete  from t_numeric;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlDelete, parameters);

		conn.getFileWriter().writeLine("**************deleting all data verification");
		sqlSelect = "select * from t_numeric;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);

				
		conn.executeSql("drop table t_numeric;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		
	}
}
