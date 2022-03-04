package gauss.regress.jdbc.bintests;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.*;

/**
 * Test the Parameter metadata API on table Run it 3 times: On table with client
 * logic column On table with no client logic On table with no client logic with
 * no CL in the connection string
 */
public class ParameterMetaDataTestBin implements IBinaryTest {
	@Override
	public void execute(DatabaseConnection4Test conn) {
		conn.getFileWriter().writeLine(" ");
		conn.getFileWriter().writeLine("Test with client logic table");
		MetadtaUtils.create_client_logic_table(conn);
		String query = "Select * from metadata_client_logic_test_tbl where id = ? or char_col != ?;";
		runTest(conn, query);
		MetadtaUtils.drop_client_logic_tbl_data(conn);

		conn.getFileWriter().writeLine(" ");
		conn.getFileWriter().writeLine("Test with client logic connection on regular table");
		query = "Select * from metadata_simple_test_tbl where id = ? or char_col != ?;";
		MetadtaUtils.create_simple_table(conn);
		runTest(conn, query);
		MetadtaUtils.drop_simple_tbl_data(conn);

		conn.getFileWriter().writeLine(" ");
		conn.getFileWriter().writeLine("Test with regular connection on regular table");
		conn.connectWithNoCL();
		MetadtaUtils.create_simple_table(conn);
		runTest(conn, query);
		MetadtaUtils.drop_simple_tbl_data(conn);
	}

	/**
	 * Encapsulate the test into a function to run it twice 
	 * @param conn  JDBC connection
	 * @param query SQL to use
	 * @throws SQLException
	 */
	private void runTest(DatabaseConnection4Test conn, String query) {
		try {
			PreparedStatement pstmt = conn.getConnection().prepareStatement(query);
			ParameterMetaData paramMetaData = pstmt.getParameterMetaData();
			if (paramMetaData == null) {
				conn.getFileWriter().writeLine("there is no support for the ParameterMetaData");
			} else {
				conn.getFileWriter().writeLine("there is a support for the ParameterMetaData");
				int paramCount = paramMetaData.getParameterCount();
				conn.getFileWriter().writeLine("paramCount=" + paramCount);
				for (int param = 1; param <= paramCount; param++) {
					conn.getFileWriter().writeLine("param number=" + param);
					int paramMode = paramMetaData.getParameterMode(param);
					conn.getFileWriter().writeLine("param mode=" + paramMode);
					if (paramMode == ParameterMetaData.parameterModeOut) {
						conn.getFileWriter().writeLine("the parameter's mode is OUT.");
					} else if (paramMode == ParameterMetaData.parameterModeIn) {
						conn.getFileWriter().writeLine("the parameter's mode is IN.");
					} else if (paramMode == ParameterMetaData.parameterModeInOut) {
						conn.getFileWriter().writeLine("the parameter's mode is INOUT.");
					} else {
						conn.getFileWriter().writeLine("the mode of a parameter is unknown.");
					}
					conn.getFileWriter().writeLine("param type = " + paramMetaData.getParameterType(param));
					conn.getFileWriter().writeLine("param class name = " + paramMetaData.getParameterClassName(param));
					conn.getFileWriter().writeLine("param count = " + paramMetaData.getParameterCount());
					conn.getFileWriter().writeLine("param precision = " + paramMetaData.getPrecision(param));
					conn.getFileWriter().writeLine("param scale = " + paramMetaData.getScale(param));
					conn.getFileWriter().writeLine("param isNullable = " + paramMetaData.isNullable(param));
					conn.getFileWriter().writeLine("param isSugned = " + paramMetaData.isSigned(param));
					conn.getFileWriter().writeLine(" ");
					conn.getFileWriter().writeLine("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
					conn.getFileWriter().writeLine(" ");
				}
			}
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}