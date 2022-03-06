/**
 *  JDBC client for parsing SQL files runnig as jdbc but not gsql
 */
package gauss.regress.jdbc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.*;
import java.io.*;

import gauss.regress.jdbc.utils.CommandType;
import gauss.regress.jdbc.utils.DBUtils;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;
import gauss.regress.jdbc.utils.IInputFileParser;
import gauss.regress.jdbc.utils.InputFileParser;
import gauss.regress.jdbc.utils.OutFileWriter;
import gauss.regress.jdbc.utils.SQLCommand;
import gauss.regress.jdbc.utils.SyntaxError;

public class JdbcClient {
	static public final int BAD_INPUT_EXIT_CODE = 1;
	static public final int CANNOT_CONNECT_EXIT_CODE = 2;
	static public final int CANNOT_OPEN_OUTPUT_FILE_EXIT_CODE  = 3;
	static public final int CANNOT_OPEN_INPUT_FILE_EXIT_CODE  = 4;
	static public final int CANNOT_WRITE_EXIT_CODE  = 5;
	static public final int CANNOT_SHELLCMD_EXIT_CODE  = 6;
	
	static private OutFileWriter m_outputFile = null;
		
	/**
	 * provide usage information
	 */
	private static void help() {
		m_outputFile.writeLine("jdbc_client.jar server_address port databs_ename user_name password input_file output_file");
		m_outputFile.writeLine("Example: jdbc_client.jar localhost 28777 regression jdbc_regress 1q@W3e4r */sql/cl_varchar.sql  */results/cl_varchar.out");
	}

	/**
	 * Parse, execute and writes output for the a sql file
	 * @param conn database connection
	 * @param inputFileName input file path
	 */
	private static void executeFile(DatabaseConnection4Test conn, String inputFileName) {
		IInputFileParser sqlFileParser = new InputFileParser();
		if (!sqlFileParser.load(inputFileName)) {
			m_outputFile.writeLine("failed opening input file: " + inputFileName);
			System.exit(CANNOT_OPEN_INPUT_FILE_EXIT_CODE);
		}
		CommandType lastCommandType = CommandType.EMPTY;
		while (sqlFileParser.moveNext()) {
			SQLCommand action = sqlFileParser.get();
			if (action != null) {	
				switch (action.getCommandType()) {
				case SHELL:
					conn.gsKtoolExec(action.getCommand());
					break;
				case COMMENT:
					m_outputFile.writeLine(action.getCommand());
					break;
				case EMPTY:
					if (lastCommandType.equals(CommandType.EXECUTE)) {
						//This is really nasty, but that's what gsql is doing
						if (action.getCommand().length() > 0) {
							m_outputFile.writeLine(action.getCommand());
						}
					}
					break;
				case EXECUTE:
					conn.executeSql(action.getCommand());
					break;
				case SELECT:
					conn.fetchData(action.getCommand());
					break;
				case DESCRIBE:
					m_outputFile.writeLine(action.getCommand());
					conn.describeObject(action.getCommand());
					break;
				case DESCRIBE_FUNCTION:
					m_outputFile.writeLine(action.getCommand());
					//conn.describeFunction(action.getCommand());
					break;
				default:
					System.out.println("Unknown command type");
					break;
				}
			}
			lastCommandType = action.getCommandType();
		}
	}
	/**
	 * execute binary tests - test that is java code
	 * @param[in] con4Test connection to the database with some utility functions
	 * @param[in] testName the name of the   
	 */
	private static void executeBinTest(DatabaseConnection4Test con4Test, String testName) {
		String className = "gauss.regress.jdbc.bintests." + testName;
		try {
			IBinaryTest test = (IBinaryTest)Class.forName(className).newInstance();
			test.execute(con4Test);
			
		} catch (InstantiationException e) {
			e.printStackTrace();
			con4Test.getFileWriter().writeLine("Test failed with InstantiationException");
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			con4Test.getFileWriter().writeLine("Test failed with IllegalAccessException");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			con4Test.getFileWriter().writeLine("Test failed with ClassNotFoundException");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int EXPECTED_PARAMETERS_COUNT = 7;
		if (args.length != EXPECTED_PARAMETERS_COUNT) {
			help();
			System.exit(BAD_INPUT_EXIT_CODE);
		}

		String serverAddress = args[0];
		String port = args[1];
		String databseName = args[2];
		String username = args[3];
		String password = args[4];
		String inputFile = args[5];
		String outputFile = args[6];
		DatabaseConnection4Test con4Test = null;
		try {
			m_outputFile = new OutFileWriter();
			m_outputFile.openFile(outputFile);
			con4Test = new DatabaseConnection4Test(m_outputFile);
			if (!con4Test.connect(serverAddress, port, databseName, username, password)) {
				m_outputFile.writeLine("database connection failed");
				System.exit(CANNOT_CONNECT_EXIT_CODE);				
			}
			if (inputFile.endsWith("Bin")) {
				executeBinTest(con4Test, inputFile);
			}
			else {
				executeFile(con4Test, inputFile);	
			}
			m_outputFile.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(CANNOT_OPEN_OUTPUT_FILE_EXIT_CODE);
		}
		if (con4Test != null) {
			con4Test.close();	
		}
	}
}
