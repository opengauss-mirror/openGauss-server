package gauss.regress.jdbc.utils;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.bintests.BinUtils;

public class MetadtaUtils {
	public static void create_simple_table(DatabaseConnection4Test conn) {
		conn.executeSql(
				"create table metadata_simple_test_tbl (key int , id int primary key, char_col varchar(30), float_col float);");
		conn.getFileWriter().writeLine("*******inserting data to the metadata_simple_test_tbl");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "insert into metadata_simple_test_tbl (key, id, char_col, float_col ) values (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("2");
		parameters.add("test_data_4_meta_data");
		parameters.add("1.1");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		String sqlSelect;
		conn.getFileWriter().writeLine("*************verifying data");
		sqlSelect = "select * from metadata_simple_test_tbl;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
	}
	
	public static void create_client_logic_table(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS metadata_client_logic_test_tbl(key int,"
				+ "id int PRIMARY KEY ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "char_col varchar(30)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "float_col float ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC))");
		conn.getFileWriter().writeLine("*******inserting data to the metadata_client_logic_test_tbl;");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "insert into metadata_client_logic_test_tbl (key, id, char_col, float_col ) values (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("2");
		parameters.add("test_data_4_meta_data");
		parameters.add("1.1");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		String sqlSelect;
		conn.getFileWriter().writeLine("*************verifying data");
		sqlSelect = "select * from metadata_client_logic_test_tbl;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
	}
		public static void drop_client_logic_tbl_data(DatabaseConnection4Test conn) {
		conn.executeSql("drop table metadata_client_logic_test_tbl;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1 CASCADE;");
	}

	public static void drop_simple_tbl_data(DatabaseConnection4Test conn) {
		conn.executeSql("drop table metadata_simple_test_tbl;");
	}
}
