package gauss.regress.jdbc.bintests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.*;

/**
 * Test the Simple SQL queries Run it 3 times: On table with client logic column
 * On table with no client logic On table with no client logic with no CL in the
 * connection string
 */
public class SimpleQueryBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		MetadtaUtils.create_client_logic_table(conn);
		MetadtaUtils.create_simple_table(conn);
		try {
			String query;
			conn.getFileWriter().writeLine("Testing table with client logic ...");
			query = "select * from metadata_client_logic_test_tbl;";
			runTest(conn, query);
			MetadtaUtils.drop_client_logic_tbl_data(conn);
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("Testing table with no client logic ...");
			query = "select * from metadata_simple_test_tbl;";
			runTest(conn, query);
			conn.getFileWriter().writeLine("Testing table with no client logic and with no client logic in connection string ...");
			conn.connectWithNoCL();
			runTest(conn, query);
		} catch (Exception e) {
			conn.getFileWriter().writeLine("Failed to execute simple sql queries");
			e.printStackTrace();
		}
		MetadtaUtils.drop_simple_tbl_data(conn);
	}

	/**
	 * Encapsulate the test into a function to run it three times
	 * 
	 * @param conn  JDBC connection
	 * @param query SQL to use
	 * @throws SQLException
	 */
	private void runTest(DatabaseConnection4Test conn, String query) throws SQLException, NullPointerException {
		Statement stmt = conn.getConnection().createStatement();
		if (stmt == null) {
			conn.getFileWriter().writeLine("Error creating statement");
			throw new NullPointerException();
		}
		stmt.execute(query);
		if (!stmt.isClosed()) {
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setCursorName()");
			stmt.setCursorName("non_existant_cursor");
			conn.getFileWriter().writeLine("setCursorName() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getWarnings()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getWarnings()));
			conn.getFileWriter().writeLine("getWarnings() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* clearWarnings()");
			stmt.clearWarnings();
			conn.getFileWriter().writeLine("clearWarnings() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getWarnings()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getWarnings()));
			conn.getFileWriter().writeLine("getWarnings() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setEscapeProcessing()");
			stmt.setEscapeProcessing(true);
			conn.getFileWriter().writeLine("setEscapeProcessing() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setFetchDirection()");
			stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
			conn.getFileWriter().writeLine("setFetchDirection() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getFetchDirection()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getFetchDirection()));
			conn.getFileWriter().writeLine("getFetchDirection() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setFetchSize()");
			stmt.setFetchSize(20);
			conn.getFileWriter().writeLine("setFetchSize() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getFetchSize()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getFetchSize()));
			conn.getFileWriter().writeLine("getFetchSize() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setMaxFieldSize()");
			stmt.setMaxFieldSize(40);
			conn.getFileWriter().writeLine("setMaxFieldSize() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getMaxFieldSize()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getMaxFieldSize()));
			conn.getFileWriter().writeLine("getMaxFieldSize() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setMaxRows()");
			stmt.setMaxRows(50);
			conn.getFileWriter().writeLine("setMaxRows() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getMaxRows()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getMaxRows()));
			conn.getFileWriter().writeLine("getMaxRows() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setPoolable()");
			stmt.setPoolable(false);
			conn.getFileWriter().writeLine("setPoolable() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* isPoolable()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.isPoolable()));
			conn.getFileWriter().writeLine("isPoolable() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* setQueryTimeout()");
			stmt.setQueryTimeout(100);
			conn.getFileWriter().writeLine("setQueryTimeout() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getQueryTimeout()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getQueryTimeout()));
			conn.getFileWriter().writeLine("getQueryTimeout() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getGeneratedKeys()");
			stmt.getGeneratedKeys();
			conn.getFileWriter().writeLine("getGeneratedKeys() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getMoreResults()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getMoreResults()));
			conn.getFileWriter().writeLine("getMoreResults() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getResultSet()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getResultSet()));
			conn.getFileWriter().writeLine("getResultSet() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getResultSetConcurrency()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getResultSetConcurrency()));
			conn.getFileWriter().writeLine("getResultSetConcurrency() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getResultSetHoldability()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getResultSetHoldability()));
			conn.getFileWriter().writeLine("getResultSetHoldability() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getResultSetType()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getResultSetType()));
			conn.getFileWriter().writeLine("getResultSetType() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getResultSetType()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getResultSetType()));
			conn.getFileWriter().writeLine("getResultSetType() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* getUpdateCount()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.getUpdateCount()));
			conn.getFileWriter().writeLine("getUpdateCount() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* cancel()");
			stmt.cancel();
			conn.getFileWriter().writeLine("cancel() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* isCloseOnCompletion()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.isCloseOnCompletion()));
			conn.getFileWriter().writeLine("isCloseOnCompletion() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* closeOnCompletion()");
			stmt.closeOnCompletion();
			conn.getFileWriter().writeLine("closeOnCompletion() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* close()");
			stmt.close();
			conn.getFileWriter().writeLine("close() executed successfully ");
			conn.getFileWriter().writeLine("");
			conn.getFileWriter().writeLine("* isClosed()");
			conn.getFileWriter().writeLine(String.valueOf(stmt.isClosed()));
			conn.getFileWriter().writeLine("isClosed() executed successfully ");
			conn.getFileWriter().writeLine("");
		} else {
			throw new SQLException();
		}
	}
}

