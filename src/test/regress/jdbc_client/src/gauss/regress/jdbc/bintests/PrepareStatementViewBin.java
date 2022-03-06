package gauss.regress.jdbc.bintests;
import java.util.ArrayList;
import java.util.List;
import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatementViewBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn){
		BinUtils.createCLSettings(conn);
		conn.executeSql("CREATE TABLE IF NOT EXISTS "
				+ "t_table_4_view(key int PRIMARY KEY ,"
				+ "col_varchar varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "col_int int  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC),"
				+ "col_float float ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		conn.getFileWriter().writeLine("*******inserting data to the t_table_4_view");
		List<String> parameters;
		String sqlInsert;
		sqlInsert = "INSERT INTO t_table_4_view (key, col_varchar, col_int, col_float) VALUES (?,?,?,?);";
		parameters = new ArrayList<>();
		parameters.add("1");
		parameters.add("data_4_view");
		parameters.add("1");
		parameters.add("1.1");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("data_4_view2");
		parameters.add("2");
		parameters.add("2.2");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		String sqlSelect;
		conn.getFileWriter().writeLine("*************verify data before creating the view");
		sqlSelect = "select * from t_table_4_view order by key;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.executeSql("CREATE View "
				+ "v_view_from_table_4_view as "
				+ "select * from t_table_4_view;");
		conn.getFileWriter().writeLine("*************verifying that new view was successfully created");
		sqlSelect = "select * from v_view_from_table_4_view order by key;";
		parameters = new ArrayList<>();
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		
		conn.getFileWriter().writeLine("**************verifying view creation");
		sqlSelect = "select *, col_int from v_view_from_table_4_view where  col_float = ? or key = ?";
		parameters = new ArrayList<>();
		parameters.add("2.2");
		parameters.add("1");
		conn.fetchDataWithPrepareStmnt(sqlSelect, parameters);
		
		conn.executeSql("drop view v_view_from_table_4_view;");
		conn.executeSql("drop table t_table_4_view;");
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		
	}		
}
		
