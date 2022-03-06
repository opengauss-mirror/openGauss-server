package gauss.regress.jdbc.bintests;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.*;

/**
 * Test the Resultset metadata API on table Run it 3 times: On table with client
 * logic column On table with no client logic On table with no client logic with
 * no CL in the connection string
 */
public class ResultSetMetaDataTestBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		MetadtaUtils.create_client_logic_table(conn);
		MetadtaUtils.create_simple_table(conn);
		try {
			conn.getFileWriter().writeLine("Testing table with client logic ...");
			String query = "Select * from metadata_client_logic_test_tbl";
			runTest(conn, query);
			MetadtaUtils.drop_client_logic_tbl_data(conn);
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("Testing table with no client logic ...");
			query = "Select * from metadata_simple_test_tbl";
			runTest(conn, query);
			conn.getFileWriter().writeLine("Testing table with no client logic and with no client logic in connection string ...");
			conn.connectWithNoCL();
			runTest(conn, query);
		} catch (Exception e) {
			conn.getFileWriter().writeLine("Failed ResutsetMetadata");
			e.printStackTrace();
		}
		MetadtaUtils.drop_simple_tbl_data(conn);
	}

	/**
	 * Encapsulate the test into a function to run it twice
	 * 
	 * @param conn  JDBC connection
	 * @param query SQL to use
	 * @throws SQLException
	 */
	private void runTest(DatabaseConnection4Test conn, String query) throws SQLException {
		Statement stmt = conn.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(query);
		ResultSetMetaData resultSetMetaData = rs.getMetaData();
		for (int i = 1; i < resultSetMetaData.getColumnCount() + 1; ++i) {
			conn.getFileWriter().writeLine("Index: " + i + " column name: " + resultSetMetaData.getColumnName(i));
			conn.getFileWriter().writeLine(" getColumnDisplaySize is: " + resultSetMetaData.getColumnDisplaySize(i));
			conn.getFileWriter().writeLine(" getColumnClassName is: " + resultSetMetaData.getColumnClassName(i));
			conn.getFileWriter().writeLine(" getColumnLabel is: " + resultSetMetaData.getColumnLabel(i));
			conn.getFileWriter().writeLine(" getColumnType is: " + resultSetMetaData.getColumnType(i));
			conn.getFileWriter().writeLine(" getColumnTypeName is: " + resultSetMetaData.getColumnTypeName(i));
			conn.getFileWriter().writeLine(" getPrecision is: " + resultSetMetaData.getPrecision(i));
			conn.getFileWriter().writeLine(" getScale is: " + resultSetMetaData.getScale(i));
			conn.getFileWriter().writeLine(" isNullable is: " + resultSetMetaData.isNullable(i));
			conn.getFileWriter().writeLine(" isNullable is: " + resultSetMetaData.isAutoIncrement(i));
			conn.getFileWriter().writeLine(" isCaseSensitive is: " + resultSetMetaData.isCaseSensitive(i));
			conn.getFileWriter().writeLine(" isCurrency is: " + resultSetMetaData.isCurrency(i));
			conn.getFileWriter().writeLine(" isReadOnly is: " + resultSetMetaData.isReadOnly(i));
			conn.getFileWriter().writeLine(" isSigned is: " + resultSetMetaData.isSigned(i));
			conn.getFileWriter().writeLine(" isWritable is: " + resultSetMetaData.isWritable(i));
			conn.getFileWriter().writeLine(" isDefinitelyWritable is: " + resultSetMetaData.isDefinitelyWritable(i));
			conn.getFileWriter().writeLine(" isSearchable is: " + resultSetMetaData.isSearchable(i));
			conn.getFileWriter().writeLine(" ");
			conn.getFileWriter().writeLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			conn.getFileWriter().writeLine(" ");
		}
	}
}