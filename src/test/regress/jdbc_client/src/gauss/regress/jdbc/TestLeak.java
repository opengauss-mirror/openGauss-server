package gauss.regress.jdbc;

// JdbcClientWrapper4LeakTest
public class TestLeak extends JdbcClientWrapper4Test{

	public static void main(String[] args) {
		String serverAddress = args[0];
		String port = args[1];
		String databseName = args[2];
		String username = args[3];
		String password = args[4];
		String testName = args[5];
		execute_test_job(testName, serverAddress, port, databseName, username, password);
		
	}
}
