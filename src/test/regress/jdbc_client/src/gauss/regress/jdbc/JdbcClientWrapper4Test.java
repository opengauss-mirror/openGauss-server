
package gauss.regress.jdbc;

import java.sql.SQLException;
import gauss.regress.jdbc.utils.DBUtils;
public class JdbcClientWrapper4Test {
	
	public static void main(String[] args) {

		String testName = "FunctionsMetadataBin";
		String port = "25632";
		String serverAddress = "localhost";
		String databaseName = "java_test";
		String userName = "jdbc_regress";
		String password = "1q@W3e4r";
		execute_test_job(testName, serverAddress, port, databaseName, userName, password);
	}

	protected static void execute_test_job(String testName, String serverAddress, String port, String databaseName,
			String userName, String password) {
		String baseLocation = System.getenv("CODE_BASE")  + "/../../distribute/test/regress/";
		String inputFile = "";
		if (testName.endsWith("Bin")){
			inputFile = testName; 
		}
		else {
			inputFile = baseLocation + "sql/" + testName + ".sql";	
		}
		String outputFile = baseLocation + "results/" + testName + ".out";
		
		try {
			DBUtils.createDatabase4Test(serverAddress, port, "postgres", userName, password, "java_test");
		} catch (SQLException e) {
			e.printStackTrace();
			return ;
		}
		
		String[] args2Send = {serverAddress, port, databaseName, userName, password, inputFile, outputFile};
		JdbcClient.main(args2Send);
	}
}
