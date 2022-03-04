package gauss.regress.jdbc.bintests;

import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.*;

/**
 * Test the Handling bad SQL queries Run it 3 times: On table with client logic
 * column On table with no client logic On table with no client logic with no CL
 * in the connection string
 */
public class HandlingBadSQLBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		MetadtaUtils.create_client_logic_table(conn);
		MetadtaUtils.create_simple_table(conn);
		List<String> queriesClList = new ArrayList<>();
		queriesClList.add("select 1* from metadata_client_logic_test_tbl;");
		queriesClList.add("from * select metadata_client_logic_test_tbl;");
		queriesClList.add("select col from metadata_client_logic_test_tbl;");
		queriesClList.add("select * frm metadata_client_logic_test_tbl;");
		queriesClList.add("select * from \"mEtadata_client_logic_test_tbl\";");
		queriesClList.add("select * from metadata_client_logic_test_tbl select * from metadata_client_logic_test_tbl;;");
		List<String> queriesNoClList = new ArrayList<>();
		queriesNoClList.add("select 1* from metadata_simple_test_tbl;");
		queriesNoClList.add("from * select metadata_simple_test_tbl;");
		queriesNoClList.add("select col from metadata_simple_test_tbl;");
		queriesNoClList.add("select * frm metadata_simple_test_tbl;");
		queriesNoClList.add("select * from \"mEtadata_simple_test_tbl\";");
		queriesNoClList.add("select * from metadata_simple_test_tbl select * from metadata_simple_test_tbl;");
		try {
			conn.getFileWriter().writeLine("Testing table with client logic ...");
			runTest(conn, queriesClList);
			conn.getFileWriter().writeLine("");
			MetadtaUtils.drop_client_logic_tbl_data(conn);
			conn.getFileWriter().writeLine("Testing table with no client logic ...");
			runTest(conn, queriesNoClList);
			conn.getFileWriter().writeLine("Testing table with no client logic and with no client logic in connection string ...");
			conn.connectWithNoCL();
			runTest(conn, queriesNoClList);
		} catch (Exception e) {
			conn.getFileWriter().writeLine("Failed to execute bad sql queries");
			e.printStackTrace();
		}
		MetadtaUtils.drop_simple_tbl_data(conn);
	}

	/**
	 * Encapsulate the test into a function to run it twice
	 * @param conn  JDBC connection
	 * @param query SQL to use
	 * @throws SQLException
	 */
	private void runTest(DatabaseConnection4Test conn, List<String> query) throws SQLException {
		for (int i = 0; i < query.size(); i++) {
			conn.fetchData(query.get(i));
			conn.getFileWriter().writeLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			conn.getFileWriter().writeLine(" ");
		}
	}
}