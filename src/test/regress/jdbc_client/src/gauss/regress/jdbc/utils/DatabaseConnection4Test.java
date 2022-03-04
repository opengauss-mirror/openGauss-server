package gauss.regress.jdbc.utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DatabaseConnection4Test implements Closeable{

	private gauss.regress.jdbc.utils.OutFileWriter m_outputFile = null;
	private Connection m_conn = null;
	private String connectionStringWithNoCL = "";
	private String connectionStringWithCL = "";
	private String username;
	private String password;
	private Boolean m_isDebug = false;
	private boolean m_is_derived = false;
	/**
	 * Constructor
	 * @param outputFile
	 */
	public DatabaseConnection4Test(OutFileWriter outputFile){
		m_outputFile = outputFile;
		m_isDebug = java.lang.management.ManagementFactory.getRuntimeMXBean().
				getInputArguments().toString().indexOf("jdwp") >= 0;
	}
	
	/**
	 * Copy Constructor alternative
	 * @param source other to copy from
	 */
	public DatabaseConnection4Test(DatabaseConnection4Test source, OutFileWriter outputFile){
		m_isDebug = java.lang.management.ManagementFactory.getRuntimeMXBean().
				getInputArguments().toString().indexOf("jdwp") >= 0;

		m_outputFile = outputFile;
		connectionStringWithNoCL = source.connectionStringWithNoCL;
		username = source.username;
		password = source.password;
		m_is_derived = true;
	}
	
	/**
	 * Closes the current JDBC connection
	 */
	private void closeConnection() {
		if (m_conn == null) {
			return;
		}

		try {
			if (!m_conn.isClosed()) {
				m_conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Connects to the database with CL and in binary mode
	 * @return true on success or false on failure
	 */
	public boolean connectWithoutReloadingCacheOnIsValid() {
		closeConnection();
		String jdbcConnectionString = connectionStringWithNoCL;
		jdbcConnectionString += "&enable_ce=1&refreshClientEncryption=0";
		try {
			m_conn = DriverManager.getConnection(jdbcConnectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Connects to the database with CL and in binary mode
	 * @return true on success or false on failure
	 */
	public boolean connectInBinaryMode() {
		closeConnection();
		String connectionString = connectionStringWithCL + "&binaryTransfer=true";
		try {
			m_conn = DriverManager.getConnection(connectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Connects to the database with client logic turned off
	 * @return true on success or false on failure
	 */
	public boolean connectWithNoCL() {
		closeConnection();
		try {
			m_conn = DriverManager.getConnection(connectionStringWithNoCL, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	/**
	 * Close and reopen the database connection with CL 
	 * @return true on success or false on failure
	 */
	public boolean reConnectWithCL() {
		closeConnection();
		try {
			m_conn = DriverManager.getConnection(connectionStringWithCL, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}	
	/**
	 * Connects to the database
	 * @param serverAddress
	 * @param port
	 * @param databseName
	 * @param username
	 * @param password
	 * @return true on success and false on failure
	 */
	public boolean connect(String serverAddress, String port,	String databseName, String username, String password) {
		this.username = username;
		this.password = password;
		//&binaryTransfer=true
		 
		String jdbcConnectionString  = 
				String.format("jdbc:postgresql://%s:%s/%s", 
						serverAddress, port, databseName);
		
		//loggerLevel=OFF&binaryTransfer=true
		if (m_isDebug) {
			jdbcConnectionString += "?loggerLevel=OFF";//"?loggerLevel=DEBUG";	
		}
		else {
			jdbcConnectionString += "?loggerLevel=OFF";
		}
		connectionStringWithNoCL = jdbcConnectionString;
		connectionStringWithCL = jdbcConnectionString + "&enable_ce=1";
		try {
			m_conn = DriverManager.getConnection(connectionStringWithCL, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * ReConnects to the database (may be called by a cloned, unconnected connection)
	 * @return true on success and false on failure
	 */
	public boolean reconnect() {
		if (connectionStringWithNoCL.isEmpty()) {
			return false;
		}
		String jdbcConnectionString = connectionStringWithNoCL;
		jdbcConnectionString += "&enable_ce=1";
		try {
			if(m_conn != null) {
				m_conn.close();
			}
			m_conn = DriverManager.getConnection(jdbcConnectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * @return the m_conn
	 */
	public Connection getConnection() {
		return m_conn;
	}
	
	@Override
	public void close(){
		if (m_conn != null) {
			try {
				m_conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (!m_is_derived && m_outputFile != null) {
			try {
				m_outputFile.close();
				m_outputFile = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if (m_outputFile != null){
			m_outputFile.flush();
		}
	}

	public OutFileWriter getFileWriter() {
		return m_outputFile;
	}

	/**
	 * execute shell command of key tool and print to the output file
	 * @param conn database connection
	 * @param command input shell command
	 */
	public void gsKtoolExec(String command) {
		m_outputFile.writeLine(command);
		Process process = null;
		try {
			String[] commandShell = command.substring(2).split("&&");
			for (String c : commandShell) {
				process = Runtime.getRuntime().exec(c);
				BufferedReader pcReader = new BufferedReader(new InputStreamReader(process.getInputStream()));

				List<String> results = new ArrayList<>();
				String outputline = "";
				while ((outputline = pcReader.readLine()) != null) {
					results.add(outputline);
				}

				String[] strs = results.toArray(new String[results.size()]);
				for (String s : strs) {
					m_outputFile.writeLine(s);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * fetch data from the database and prints it to the output file
	 * @param conn database connection
	 * @param sql SQL statement to invoke
	 */
	public void fetchData(String sql) {
		m_outputFile.writeLine(sql);
		Statement st = null;
		try {
			st = m_conn.createStatement();
			if (st == null || sql == null) {
				System.out.println("found null");
			}
			ResultSet rs = null;
			rs = st.executeQuery(sql);
			printRS4Test(rs);
			st.close();
		} catch (SQLException e) {
			if (m_isDebug) {
				e.printStackTrace();
			}
			String errorMessage = e.getMessage();
			printError(errorMessage, sql);
		}
	}

	/**
	 * Executes SQL and prints any warning / errors to the output file
	 * @param conn
	 * @param sql
	 */
	public void executeSql(String sql) {
		Statement st = null;
		try {
			sql = sql.replace("\n\n", "\n").replace("\n\n", "\n");
			m_outputFile.writeLine(sql);
			st = m_conn.createStatement();
			st.executeUpdate(sql);
			displayQueryWarning(st);
			st.close();
		} catch (SQLException e) {
			if  (st!= null) {
				try {
					displayQueryWarning(st);
				} catch (SQLException e1) {
				}
			}
			if (m_isDebug) {
				e.printStackTrace();
			}			
			String errorMessage = e.getMessage();			
			printError(errorMessage, sql);
			return;
		}
	}	
	/**
	 * Fetch data using prepare statement and print results to the writer
	 * @param sql the statement sql
	 * @param parameters parameters 
	 */
	public void fetchDataWithPrepareStmnt(String sql, List<String> parameters) {
		try {
			m_outputFile.writeLine(sql);
			PreparedStatement statement = m_conn.prepareStatement(sql);
			int i = 0;
			for (String param: parameters) {
				if (i > 0) {
					m_outputFile.write(",");
				}
				++i;
				m_outputFile.write(param);
				statement.setString(i, param);
			}
			m_outputFile.writeLine("");
			ResultSet rs = statement.executeQuery();
			printRS4Test(rs);
			statement.close();
		} catch (SQLException e) {
			String errorMessage = e.getMessage();			
			printError(errorMessage, sql);
			return;
		}
	}	

	/**
	 * Execute prepare statement 
	 * @param[in] sql the query
	 * @param[in] parameters parameter values
	 * @param expexetedAffectedRecords
	 */
	public void updateDataWithPrepareStmnt(String sql, List<String> parameters) {
		try {
			m_outputFile.writeLine(sql);
			PreparedStatement statement = m_conn.prepareStatement(sql);
			int i = 0;
			for (String param: parameters) {
				if (i > 0) {
					m_outputFile.write(",");
				}
				++i;
				if (param != null) {
					m_outputFile.write(param);
				}
				statement.setString(i, param);
			}
			m_outputFile.writeLine("");
			statement.executeUpdate();
			statement.close();
		} catch (SQLException e) {
			String errorMessage = e.getMessage();			
			printError(errorMessage, sql);
			return;
		}
	}	

	/**
	 * Describes database object into the output file 
	 * @param conn database connection
	 * @param command \d command in gsql style
	 */
	public void describeObject(String command) {
		int pos = command.indexOf("d");
		String describeObjectName = "";
		if (pos < command.length() + 1) {
			describeObjectName = command.substring(pos+1).trim();
		}
		describeObjectName = describeObjectName.replace(";", "");
		describeObjectName = describeObjectName.replace("+", "");//Not supported yet
		describeObjectName = describeObjectName.trim();
		try {
			DatabaseMetaData metadata = m_conn.getMetaData();
			ResultSet columnsRS = metadata.getColumns("", "", describeObjectName, "");
			String schemaName = "public";
			List<List<String>> data = new ArrayList<>();
			
			while (columnsRS.next()) {
				List<String> record = new ArrayList<>();
				record.add(columnsRS.getString("COLUMN_NAME"));
				String type_name = getDBTypeDisplayName(columnsRS.getString("TYPE_NAME"));
				record.add(type_name);

				String modifiers = "";
				if (columnsRS.getString("COLUMN_DEF") != null) {
					modifiers += " default " + columnsRS.getString("COLUMN_DEF");
				}
				if (columnsRS.getString("IS_NULLABLE").equals("NO")) {
					modifiers += "not null";
				}
				if (columnsRS.getString("SQL_DATA_TYPE") != null && columnsRS.getString("SQL_DATA_TYPE") != "null") {
					modifiers += " encrypted";
				}
				record.add(modifiers);
				data.add(record);
			}
			List<String> headers = Arrays.asList("Column", "Type", "Modifiers");
			List<String> columnTypes = Arrays.asList(String.class.getClass().getName(), String.class.getClass().getName(), 
					String.class.getClass().getName()); 

	        String header = "Table " + "\"" + schemaName + "." + describeObjectName + "\"";
			if (!data.isEmpty()) {
				printDataAsTables(headers, columnTypes, data, header, false);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
     /**
	 * Describes database object into the output file 
	 * @param conn database connection
	 * @param command \d command in gsql style
	 */
	public void describeFunction(String command) {
		int pos = command.indexOf("df");
		String describeFunctionName = "";
		if (pos < command.length() + 1) {
			describeFunctionName = command.substring(pos+1).trim();
		}
		describeFunctionName = describeFunctionName.replace(";", "");
		describeFunctionName = describeFunctionName.replace("+", "");//Not supported yet
		describeFunctionName = describeFunctionName.trim();
		try {
			DatabaseMetaData metadata = m_conn.getMetaData();
			ResultSet columnsRS = metadata.getFunctions("", "", describeFunctionName);
			String schemaName = "public";
			List<List<String>> data = new ArrayList<>();
			
			while (columnsRS.next()) {
				List<String> record = new ArrayList<>();
				record.add(columnsRS.getString("COLUMN_NAME"));
				record.add(getDBTypeDisplayName(columnsRS.getString("TYPE_NAME")));
				if (columnsRS.getString("SQL_DATA_TYPE") != null && columnsRS.getString("SQL_DATA_TYPE") != "null"){
					record.add(" clientlogic");	
				}
				else {
					record.add("");
				}
				data.add(record);
			}
			List<String> headers = Arrays.asList("Schema", "Name", "Result data type", "Argument data types ", "Type");
			List<String> columnTypes = Arrays.asList(String.class.getClass().getName(), String.class.getClass().getName(), 
					String.class.getClass().getName()); 

	        String header = "Table " + "\"" + schemaName + "." + describeFunctionName + "\"";
			printDataAsTables(headers, columnTypes, data, header, false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	/**
	 * Converts the field database type returned from JDBC to those displayed in gsql in \d command 
	 * @param dbTypeName JDBC type name
	 * @return the gsql displayed name
	 */
	private static String getDBTypeDisplayName(String dbTypeName) {
		
		switch (dbTypeName){
			case "int1":	
			case "int2":
			case "int4":
			case "int8":
				return "integer";
				
			case "varchar":
				return "character varying";
				
			case "numeric":
				return "numeric";
		}
		return dbTypeName;
	}	
	/**
	 * Prints the content of a result set to the output file
	 * @param rs
	 */
	public void printRS4Test(ResultSet rs) {
		printRS4Test(rs, null);
	}
	public void printRS4Test(ResultSet rs, Set<String> skipColumns) {
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsCount = rsmd.getColumnCount();
			List<String> headers = new ArrayList<>();
			List<String> types = new ArrayList<>();
			List<List<String>> data = new ArrayList<>();
			Set<Integer> skipColumnIndexes = new HashSet<>();
			//fill data in headers and types
			for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
				headers.add(rsmd.getColumnName(colIndex));
				types.add(rsmd.getColumnClassName(colIndex));
				if (skipColumns != null && skipColumns.contains(rsmd.getColumnName(colIndex))) {
					skipColumnIndexes.add(colIndex);
				}
			}
			while (rs.next()){
				List<String> record = new ArrayList<>();
				for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
					String colValue = rs.getString(colIndex);
					if (colValue == null || skipColumnIndexes.contains(colIndex)) {
						colValue = "";
					}
					record.add(colValue);
				}
				data.add(record);
			}

			printDataAsTables(headers, types, data, "", true);
		} catch (SQLException e1) {
			m_outputFile.writeLine("Exception in printRS4Test");
			e1.printStackTrace();
			return;
		}		
	}
	
	/**
	 * Prints tabular data to the output file 
	 * @param headers table header names
	 * @param columnTypes the types of the columns
	 * @param data the table data
	 * @param tableHeader header to print on top of the table
	 * @param writeFooter if to write table footer
	 */
	public void printDataAsTables(List<String> headers, List<String> columnTypes, List<List<String>> data, 
			String tableHeader, boolean writeFooter) {
		List<Integer> maxColumnValueLength = new ArrayList<>();
		int columnsCount = headers.size();
		if (columnTypes.size() != columnsCount) {
			System.out.println("Invalid column headers size, exit printDataAsTables!");
			return;
		}
		// Calculate the maximum header width per column
		for (String header : headers) {
			maxColumnValueLength.add(StringUtils.getStringWidth(header));
		}
		// Calculate the maximum column using the actual data
		for (int recordIndex = 0; recordIndex < data.size(); ++recordIndex) {
			List<String> record = data.get(recordIndex);
			if (record.size() != columnsCount) {
				System.out.println("Invalid record size, exit printDataAsTables!");
				return;
			}
			int colIndex = 0;
			for (String colValue : record) {
				int width = StringUtils.getStringWidth(colValue);
				if (width > maxColumnValueLength.get(colIndex)) {
					maxColumnValueLength.set(colIndex, width);
				}
				++colIndex;
			}							
		}
		String space = " ";
		String dash = "-";
		String connector = "+";
		String sep = "|";

		if (tableHeader.length() > 0) {
			int tableWidth = maxColumnValueLength.stream().mapToInt(Integer::intValue).sum();
			tableWidth += headers.size()*2;// space before and space after 
			tableWidth += headers.size() - 1; // sperator between columns  
			StringBuilder headerLine = new StringBuilder();
			addValueToLine(headerLine, tableHeader, tableWidth - 2, space, "C", true);
			m_outputFile.writeLine(headerLine.toString());
		}
		
		// Print the result set headers;
		StringBuilder lineHeaders = new StringBuilder();
		for (int colIndex = 0; colIndex < columnsCount ; ++colIndex) {
			String colName = headers.get(colIndex);
			addValueToLine(lineHeaders, colName,maxColumnValueLength.get(colIndex), space, "C", false);
			if (colIndex != columnsCount - 1) {
				lineHeaders.append(sep);	
			}
		}
		m_outputFile.writeLine(lineHeaders.toString());
		StringBuilder lineSep = new StringBuilder();
		for (int colIndex = 0; colIndex < columnsCount; ++colIndex) {
			addValueToLine(lineSep, "", maxColumnValueLength.get(colIndex), dash, "C", false);
			if (colIndex != columnsCount - 1) {
				lineSep.append(connector);
			}
		}	
		m_outputFile.writeLine(lineSep.toString());
		for (List<String> record : data) {
			StringBuilder lineData = new StringBuilder();
			for (int colIndex = 0; colIndex < columnsCount; ++colIndex) {
				String aligment = "L";
				if (columnTypes.get(colIndex).equals("java.lang.Integer") || 
						columnTypes.get(colIndex).equals("java.lang.Long") ||
						columnTypes.get(colIndex).equals("java.math.BigDecimal")) {
					aligment = "R";
				}
				boolean isLast = false;
				if (colIndex == columnsCount - 1) {
					isLast = true;
				}
				addValueToLine(lineData, record.get(colIndex), maxColumnValueLength.get(colIndex), space, 
						aligment, isLast);
				if (!isLast) {
					lineData.append(sep);	
				}
			}
			m_outputFile.writeLine(lineData.toString());
		}
		if (writeFooter) {
			if (data.size() == 1) {
				m_outputFile.writeLine("(1 row)");
			}
			else { //if (data.size() > 0)
				m_outputFile.writeLine("(" + data.size() + " rows)");
			}			
		}
		m_outputFile.writeLine("");
	}
	/**
	 * Add field value to a line while printing a table to the output file 
	 * @param line current line to add to
	 * @param value the value to print
	 * @param MaxLen maximum column length
	 * @param space padding character
	 * @param aligmnet R - right, C - center, L - left
	 * @param lastField is last field in the line
	 */
	private void addValueToLine(StringBuilder line, String value, int MaxLen, String space, String aligmnet, boolean lastField) {
		// line.append(space);
		int numberOfPrefixSpaces = 1;
		int valueWidth = StringUtils.getStringWidth(value);
		switch (aligmnet){
			case "C": // center
				numberOfPrefixSpaces = (MaxLen - valueWidth) / 2 + 1;
				break;
			case "L": // left
				numberOfPrefixSpaces = 1;
				break;
			case "R": // Right
				numberOfPrefixSpaces = 1 + MaxLen - valueWidth;
				
		}
		for (int i = 0; i < numberOfPrefixSpaces; ++i) {
			line.append(space);
		}
		line.append(value);
		if (!lastField) {
			for (int i = numberOfPrefixSpaces +  valueWidth; i < MaxLen + 2; ++i) {
				line.append(space);
			}			
		}
	}	
	/**
	 * gets SQL with syntax error and return the text to apear for syntax error LINE 1" ...SQL...
	 * Similar functionality to reportErrorPosition function in libPQ
	 * @param sql
	 * @param poistion
	 * @return structure with SQLline and positon in that line
	 */
	private static SyntaxError getSynatxErrorLineAndPosition(String sql, int position) {
		final int MAX_WIDTH = 60;
		final int MIN_RIGHT_WIDTH = 10;
		final String GAP = "..."; 
		int lineNumber = 0;
		SyntaxError returnValue = new SyntaxError();
		String[] lines = sql.split("\n");
		String sqlLine = "";
		int positionInLine = 0;
		if (position <  lines[0].length()) {
			lineNumber = 1;
			returnValue.setPositionInLine(position);
			positionInLine = position;
			sqlLine = sql;
		}
		else{
			// Find the line with error and the position in that line
			int acumPosition = 0;
			boolean found = false;
			for (int lineCounter = 0; lineCounter < lines.length && !found; ++lineCounter) {
				if (position < acumPosition + lines[lineCounter].length()) {
					lineNumber = lineCounter + 1;
					positionInLine = position - acumPosition;
					sqlLine = lines[lineCounter];
					found = true;
				}
				else {
					acumPosition += lines[lineCounter].length() + 1;//1 for the \n character
				}
			}
		}
		String linePrefix = "LINE " + lineNumber  + ": ";
		if (sqlLine.length() > MAX_WIDTH) {
			//The part with syntax error is at the beginning
			if (positionInLine < MAX_WIDTH - MIN_RIGHT_WIDTH) {
				sqlLine = sqlLine.substring(0, 60) + GAP;
			}
			else{
				//The part with syntax error is at the middle:
				//For Example- ...CLIENT_LOGIC GLOBAL_SETTING test_drop_cmk2 WITH ( KEY_STORE ...
				//                                                                  ^
				if (sqlLine.length() > positionInLine + MIN_RIGHT_WIDTH) {
					sqlLine = GAP + sqlLine.substring(positionInLine - 50 -1, positionInLine + MIN_RIGHT_WIDTH - 1) + GAP;
					positionInLine = MAX_WIDTH - MIN_RIGHT_WIDTH + GAP.length() + 1;					
				}
				//The part with syntax error is at the end:
				//For Example- ...CLIENT_LOGIC GLOBAL_SETTING test_drop_cmk2 WITH (KEY_STORE)
				//                                                                  ^
				else {
					sqlLine = GAP + sqlLine.substring(sqlLine.length() - MAX_WIDTH);
					positionInLine = positionInLine + MAX_WIDTH - sqlLine.length();										
				}
			}
		}
		returnValue.setPositionInLine(positionInLine + linePrefix.length() - 1);
		returnValue.setSqlLine(linePrefix + sqlLine);  
		return returnValue;
	}
	/**
	 * prints syntax error in gsql style if any
	 * @param messages the error messages from the server  
	 * @param sql the statement caused the error / warning 
	 * @param outputLines List of errors to print
	 * @return true if it is a syntax error and false if it is not
	 */
	private static boolean printSyntaxError(String[] messages, String sql, List<String> outputLines) {
		
		String message = "";
//		if (sql.contains("\n")) {//Do not handle syntax errors in multi-line SQLs - too complex
//			return false;
//		}
		//Looking for the position message for syntax error
		for (int msgIndex = 0; msgIndex < messages.length; ++msgIndex) {
			String[] items = messages[msgIndex].split(":");
			if (items.length > 1) {
				if (items[0].trim().toUpperCase().equals("POSITION")){
					message = messages[msgIndex];
					break;
				}
			}
		}
		if (message.length() == 0) {
			return false;//No position message
		}
		boolean isSyntaxError = false;
		String[] items = message.split(":");
		if (items.length == 2) {
			try {
				int position = Integer.parseInt(items[1].trim());
				SyntaxError sqlLine = getSynatxErrorLineAndPosition(sql, position);
				outputLines.add(sqlLine.getSqlLine());
				StringBuilder errorLine2 = new StringBuilder();
				for (int index = 0; index < sqlLine.getPositionInLine(); ++index) {
					errorLine2.append(" ");
				}
				errorLine2.append("^");
				outputLines.add(errorLine2.toString());
				isSyntaxError = true;
			}
			catch (NumberFormatException e) {
				//do nothing here, just catch it and isSyntaxError  is false; 
			}
		}
		return isSyntaxError;
	}
	/**
	 * prints an error to the output file. Some changes are required to match gsql & libpq formatting of the message 
	 * @param message the error message
	 * @param sql the statement caused the error / warning 
	 */
	private void printError(String message, String sql) {
		int MAX_WARNING_HEADER = 10;//Warning header is short string like NOTICE, INFO HINT
		// Server error contains prefix with [connection details] - remove it
		if (message.length() == 0) {
			return;
		}
		if (message.charAt(0) == '[') {
			int secondBracket = message.indexOf("]");
			if (secondBracket > 0){// Cannot be zero as character in index zero is '['
				if (message.length() > secondBracket) {
					message = message.substring(secondBracket + 1);
				}
			}
		}
		String[] messages = message.split("\n");
		// For some reason the output in gsql and jdbc of the warning is a bit different, changing it here
		boolean didPrintSyntaxError = false;
		List<String> outputLines = new ArrayList<>();
		int lineNumber = 0;
		for (String item : messages ) {
			++lineNumber;
			String[] itemSplited = item.split(":");
			if (itemSplited.length > 1) {
				if (lineNumber == 2) {// print syntax error as 2nd line if there is syntax error
					didPrintSyntaxError = printSyntaxError(messages, sql, outputLines);
				}
				if (itemSplited[0].trim().length() < MAX_WARNING_HEADER) {
					String errorHeader = itemSplited[0].trim().toUpperCase();
					if (errorHeader.equals("WHERE")){
						errorHeader = "CONTEXT";
					}
					boolean skipThisPart = false;
					if (errorHeader.equals("POSITION")) {
						skipThisPart = didPrintSyntaxError;
					}
					if (!skipThisPart) {
						StringBuilder lineOutput = new StringBuilder();
						lineOutput.append(errorHeader);
						lineOutput.append(":  ");
						lineOutput.append(itemSplited[1].trim());
						// If there is more ':' character, just write it etc.
						for (int warningPart = 2; warningPart < itemSplited.length; ++warningPart) {
							lineOutput.append(":");
							lineOutput.append(itemSplited[warningPart]);
						}
						// writeLine("");
						outputLines.add(lineOutput.toString());						
					}
				}
				else {
					outputLines.add(item);
					// writeLine(item);
				}
			}
			else {
				outputLines.add(item);
				// writeLine(item);
			}
		}
		for (String outputLine : outputLines) {
			m_outputFile.writeLine(outputLine);
		}
		if (message.charAt(message.length() - 1) == '\n') {
			m_outputFile.writeLine("");// for errors coming from libpq with \n at the end
		}
	}
	
	/**
	 * Display query warning (notice & hints supplied by the server in case of a successful query execution)
	 * @param st the statement issued
	 * @throws SQLException
	 */
	private void displayQueryWarning(Statement st) throws SQLException {
		
		SQLWarning warnings = st.getWarnings();
		while (warnings != null) {
			// warnings.getMessage() does not return the entire message so have to use reflection to get the detailedMessage field
			try {
				Field f = Throwable.class.getDeclaredField("detailMessage");
				f.setAccessible(true);
				String message = (String) f.get(warnings); // IllegalAccessException
				printError(message, "");
			} catch (NoSuchFieldException e) {
				m_outputFile.writeLine("failed resing detailMessage - NoSuchFieldException");
				e.printStackTrace();
			} catch (SecurityException e) {
				m_outputFile.writeLine("failed resing detailMessage - SecurityException");
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				m_outputFile.writeLine("failed resing detailMessage - IllegalArgumentException");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				m_outputFile.writeLine("failed resing detailMessage - IllegalAccessException");
				e.printStackTrace();
			}
			warnings = warnings.getNextWarning();
		}		
	}
}
