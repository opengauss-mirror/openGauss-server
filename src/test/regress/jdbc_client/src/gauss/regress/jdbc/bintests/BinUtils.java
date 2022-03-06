package gauss.regress.jdbc.bintests;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.sun.net.httpserver.Authenticator.Result;

import gauss.regress.jdbc.utils.DBUtils;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class BinUtils {
	
	static public void getMultipleResultsets(DatabaseConnection4Test conn, String sql) {
		try {
			Statement stmt = conn.getConnection().createStatement();
			 boolean results = stmt.execute(sql);
		        do {
		        	if (results) {
		                ResultSet rs = stmt.getResultSet();
		                conn.printRS4Test(rs);
		            }
		            results = stmt.getMoreResults();
		        } while (results);				 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	static public void createCLSettings(DatabaseConnection4Test conn) {
		conn.gsKtoolExec("\\! gs_ktool -d all");
		conn.gsKtoolExec("\\! gs_ktool -g");
		String sql;
		sql = "CREATE CLIENT MASTER KEY cmk1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);";
		conn.executeSql(sql);
		sql = "CREATE COLUMN ENCRYPTION KEY cek1 WITH VALUES (CLIENT_MASTER_KEY = cmk1, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);";
		conn.executeSql(sql);
	}
	
	static public void dropCLSettings(DatabaseConnection4Test conn) {
		conn.executeSql("DROP COLUMN ENCRYPTION KEY cek1;");
		conn.executeSql("DROP CLIENT MASTER KEY cmk1;");
		conn.gsKtoolExec("\\! gs_ktool -d all");

	}

	static public void cleanCLSettings(DatabaseConnection4Test conn) {
		String sql;
		sql = "DROP COLUMN ENCRYPTION KEY cek1;";
		conn.executeSql(sql);
		sql = "DROP CLIENT MASTER KEY cmk1;";
		conn.executeSql(sql);
		conn.gsKtoolExec("\\! gs_ktool -d all");
	}
	
	/**
	 * Prints the data of a parameter
	 * @param conn connection object
	 * @param parameter the parameter value
	 * @param functionName the na,e of the server function it was fetched from 
	 * @param index the parameter index in the server function
	 */
	public static void printParameter(DatabaseConnection4Test conn, Object parameter, String functionName, int index) {
		conn.getFileWriter().writeLine(functionName + " value of index " + index + 
				" Type is " + parameter.getClass().getName() + " value is " + parameter);
	}
	
	/**
	 * Invoke a server function using CallableStatement and parse its output parameters
	 * @param conn connection object
	 * @param functionName method name
	 * @param numberOfParameters parameter count in the function
	 */
	static void invokeFunctionWithIntegerOutParams(DatabaseConnection4Test conn, String functionName, 
			int numberOfParameters) {
		try {
			
			conn.getFileWriter().writeLine("Invoking " + functionName + " using CallableStatement:");
			StringBuilder command = new StringBuilder();
			command.append("{call ");
			command.append(functionName);
			command.append("(");
			for (int index = 0; index < numberOfParameters; ++index) {
				if (index > 0) {
					command.append(",");
				}
				command.append("?");
			}
			command.append(")}");
			CallableStatement callStmnt = conn.getConnection().prepareCall(command.toString());
			for (int index = 0; index < numberOfParameters; ++index) {
				callStmnt.registerOutParameter(index + 1, Types.INTEGER);
			}
			callStmnt.execute();
			for (int index = 0; index < numberOfParameters; ++index) {
				Object data = callStmnt.getObject(index + 1);
				BinUtils.printParameter(conn, data, functionName, index);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			conn.getFileWriter().writeLine("ERROR running " + functionName + " " + e.getMessage());
		}
		
	}	
}
