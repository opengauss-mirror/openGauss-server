package gauss.regress.jdbc.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class InputFileParser implements IInputFileParser {

	String[] m_arrLines;
	int m_linesPointer = -1;
	@Override
	public boolean load(String path) {
		byte[] encoded = null;
		try {
			encoded = Files.readAllBytes(Paths.get(path));
		} catch (IOException e) {
			e.printStackTrace();
			return true;
		}
		String content = new String(encoded, StandardCharsets.UTF_8);
		m_arrLines = content.split("\n");
		return true;
	}

	@Override
	public boolean moveNext() {
		++m_linesPointer;
		return isSafeIndex(m_linesPointer);
	}

	private String extractCommand(int currentPosition) {
		if (!isSafeIndex(currentPosition)) {
			System.out.println("extractCommand - reached end of lines");
			return "";
		}
		StringBuilder result = new StringBuilder();
		int index = currentPosition;
		boolean foundSemiColumn = false;
		boolean isFirstLine = true;
		boolean isCreateFunction = false;
		boolean isFunctionEnded =  false; // helper variable for ending function oracle style
		while (!foundSemiColumn && isSafeIndex(index)&& !isFunctionEnded) {
			if (!isFirstLine) {
				result.append("\n");	
			}
			isFirstLine = false;	
			result.append(m_arrLines[index]);
			if (m_arrLines[index].startsWith("\\")) {
				++index;
				return result.toString();
			}
			// This is still very primitive statement parser, may be improved in later time 
			if (m_arrLines[index].toUpperCase().startsWith("CREATE FUNCTION")
					|| m_arrLines[index].toUpperCase().startsWith("CREATE OR REPLACE FUNCTION")
					|| m_arrLines[index].toUpperCase().startsWith("CREATE PROCEDURE")
					|| m_arrLines[index].toUpperCase().startsWith("CREATE OR REPLACE PROCEDURE")
					){
				isCreateFunction = true;
			}
			if (isCreateFunction) {
				if (m_arrLines[index].toUpperCase().contains("LANGUAGE")) {
					isCreateFunction = false;
				}
				else if (m_arrLines[index].toUpperCase().trim().equals("/")) {
					/*in oracle style functions, the function ends with slash. for instance:
					 * create or replace function select1() RETURN SETOF t_num
						AS
						BEGIN
    					return query (SELECT * from t_num);
						END;
						/
					 * 
					 */
					isFunctionEnded = true;
				}
			}
		    if (!isCreateFunction && m_arrLines[index].contains(";")) {
				foundSemiColumn = true;
			}
			m_linesPointer = index;
			++index;
		}
		return result.toString();
	}
	private boolean isSafeIndex(int index) {
		if (index < m_arrLines.length) {
			return true;
		}
		return false;
	}
	@Override
	public SQLCommand get() {
		if (!isSafeIndex(m_linesPointer)) {
			System.out.println("SQLCommand get - reached end of lines");
			return null;
		}
		SQLCommand command = null;
		// Comments:
		if (m_arrLines[m_linesPointer].trim().startsWith("--") 
				|| m_arrLines[m_linesPointer].trim().startsWith("\\set")
				|| m_arrLines[m_linesPointer].trim().equals("\\d")
				|| m_arrLines[m_linesPointer].trim().startsWith("\\sf")){
			command = new SQLCommand(m_arrLines[m_linesPointer], CommandType.COMMENT);
			return command ;
		}
		if (m_arrLines[m_linesPointer].trim().startsWith("\\!")) {
			command = new SQLCommand(m_arrLines[m_linesPointer], CommandType.SHELL);
			return command ;
		}
		// Empty lines
		if (m_arrLines[m_linesPointer].trim().length() == 0) {
			command = new SQLCommand(m_arrLines[m_linesPointer], CommandType.EMPTY);
			return command ;
		}
		// Describe table
		if (m_arrLines[m_linesPointer].trim().startsWith("\\d ") || m_arrLines[m_linesPointer].trim().startsWith("\\d+")) {
			command = new SQLCommand(m_arrLines[m_linesPointer], CommandType.DESCRIBE);
			return command;
		}
		// Describe function
		if (m_arrLines[m_linesPointer].trim().startsWith("\\df")) {
			command = new SQLCommand(m_arrLines[m_linesPointer], CommandType.DESCRIBE_FUNCTION);
			return command;
		}
		// Extract the SQL itself
		String sql = extractCommand(m_linesPointer);
		String normSQL = sql.toLowerCase().trim(); 
		if (normSQL.startsWith("select") ||
				normSQL.startsWith("with") ||
				normSQL.startsWith("explain") ||
				normSQL.startsWith("show") ||
				normSQL.startsWith("fetch") ||
				normSQL.startsWith("call")){
			command = new SQLCommand(sql, CommandType.SELECT);
		}
		else {
			command = new SQLCommand(sql, CommandType.EXECUTE);
		}
		return command;
	}

}
