package gauss.regress.jdbc.bintests;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.*;

/**
 * Test the batch queries on the client logic table 
 */
public class StmtBatchClientLogicBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		MetadtaUtils.create_client_logic_table(conn);
		int[] count;
		String query;
		try {
			Statement stmt = conn.getConnection().createStatement();
			List<String> parameters;
			query = "Select * from metadata_client_logic_test_tbl order by id desc;";
			stmt.addBatch(query);
			query = "insert into metadata_client_logic_test_tbl values (10,20,'new data', 5.5 );";
			stmt.addBatch(query);
			query = "insert into metadata_client_logic_test_tbl values (30,-40,'new data 2', -6.6 );";
			stmt.addBatch(query);
			query = "update metadata_client_logic_test_tbl set char_col = 'new_data was updated' where key = 10;";
			stmt.addBatch(query);
			count = stmt.executeBatch();
			conn.getConnection().commit();
			conn.getFileWriter().writeLine("verifying the batch cmd");
			query = "select * from metadata_client_logic_test_tbl order by id desc;";
			parameters = new ArrayList<>();
			conn.fetchDataWithPrepareStmnt(query, parameters);
		} 
		catch (Exception e) {
			conn.getFileWriter().writeLine("Failed to execute batch" + e);
		}
		try {
			Statement stmt = conn.getConnection().createStatement();
			List<String> parameters;
			query = "insert into metadata_client_logic_test_tbl values (100,-400,'this data will not appear', -8.8 );";
			stmt.addBatch(query);
			query = "update metadata_client_logic_test_tbl set char_col = 'this update will not executed' where key = 10;";
			stmt.addBatch(query);
			stmt.clearBatch();
			count = stmt.executeBatch();
			conn.getFileWriter().writeLine("verifying the clear batch cmd");
			query = "select * from metadata_client_logic_test_tbl;";
			parameters = new ArrayList<>();
			conn.fetchDataWithPrepareStmnt(query, parameters);
		} 
		catch (Exception e) {
			conn.getFileWriter().writeLine("Failed to execute clear batch cmd" + e);
		}
		MetadtaUtils.drop_client_logic_tbl_data(conn);
	}
}
