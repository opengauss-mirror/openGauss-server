import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.sql.*;

public class UtilTool {


    public static void addValueToLine(StringBuilder line, String value, int MaxLen, String space, boolean centerIt) {
		//line.append(space);
		int numberOfPrefixSpaces = 1;
		if (centerIt) {
			numberOfPrefixSpaces = (MaxLen - value.length()) / 2 + 1;
		}
		for (int i = 0; i < numberOfPrefixSpaces; ++i) {
			line.append(space);
		}
		line.append(value);
		for (int i = numberOfPrefixSpaces +  value.length(); i < MaxLen + 2; ++i) {
			line.append(space);
		}
	
	}

	public static void printRS4Test(ResultSet rs) {
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsCount = rsmd.getColumnCount();
			List<Integer> maxColumnValueLength = new ArrayList<>();
			//Calculate the 
			for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
				maxColumnValueLength.add(rsmd.getColumnName(colIndex).length());
			}
			List<List<String>> data = new ArrayList<>();
			while (rs.next()){
				List<String> record = new ArrayList<>();
				for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
					String colValue = rs.getString(colIndex);
					if (colValue!= null) {
						if (colValue.length() > maxColumnValueLength.get(colIndex - 1)) {
							maxColumnValueLength.set(colIndex - 1, colValue.length());
						}
						record.add(colValue);
					}
					else {
						record.add("null");
					}
					
				}
				data.add(record);
			}

			String space = " ";
			String dash = "-";
			String connector = "+";
			String sep = "|";
			//Print the result set headers;
			StringBuilder lineHeaders = new StringBuilder();
			for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
				String colName = rsmd.getColumnName(colIndex);
				addValueToLine(lineHeaders, colName,maxColumnValueLength.get(colIndex - 1), space, true);
				if (colIndex != columnsCount) {
					lineHeaders.append(sep);	
				}
			}
			System.out.println(lineHeaders);
			StringBuilder lineSep = new StringBuilder();
			for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
				addValueToLine(lineSep, "", maxColumnValueLength.get(colIndex - 1), dash, true);
				if (colIndex != columnsCount) {
					lineSep.append(connector);
				}
			}	
			System.out.println(lineSep);
			for (List<String> record : data) {
				StringBuilder lineData = new StringBuilder();
				for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
					addValueToLine(lineData, record.get(colIndex - 1), maxColumnValueLength.get(colIndex - 1), space, false);
					if (colIndex != columnsCount) {
						lineData.append(sep);	
					}
				}
				System.out.println(lineData);
			}
			if (data.size() == 0) {
				System.out.println("(0 row)");
			}
			else{
				System.out.println("(" + data.size() + " rows)");
			}
			System.out.println("");
			
		} catch (SQLException e1) {
			e1.printStackTrace();
			return;
		}

		ResultSetMetaData rsmd;
		try {
			rsmd = rs.getMetaData();
		} catch (SQLException e1) {
			e1.printStackTrace();
			return;
		}
		int columnsNumber = 0;
		try {
			columnsNumber= rsmd.getColumnCount();
		} catch (SQLException e1) {
			e1.printStackTrace();
            return;
		}
		/*for (int i = 1 ; i < columnsNumber + 1; ++i) {
			try {
				System.out.print(rsmd.getColumnName(i));
			} catch (SQLException e) {
				e.printStackTrace();
                return;
			}
			System.out.print("  ");
		}*/
		final String line_sep = "------------------------------";
		try {
			while (rs.next()){
				System.out.println("");
				System.out.println(line_sep);
				for (int i = 1 ; i < columnsNumber + 1; ++i) {
					String value = rs.getString(i);
					System.out.print(value);
					System.out.print("  |  ");
				}
			}
			System.out.println("");
			//System.out.println(line_sep);
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}
    	try {
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
            return;
		}
		
	}
	public static boolean fetchData(Connection conn, String sql) {
    	Statement st = null;
		try {
			st = conn.createStatement();
			ResultSet rs = null;
			rs = st.executeQuery(sql);
			printRS4Test(rs);
            if (rs!=null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
    public static boolean executeSql(Connection conn, String sql) {
        Statement st = null;
        try {
            st = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }	
        int rowsAffected = 0;
        try {
            rowsAffected = st.executeUpdate(sql);
            SQLWarning warnings = st.getWarnings();
            if (warnings != null) {
                System.out.println("NOTICE:  " + warnings.getMessage());
                warnings.getNextWarning();
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        try {
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public static void setStmntParameters(List<Object> parameters, PreparedStatement statement) throws SQLException {
        int i = 0;
        for (Object param: parameters) {
            ++i;
            if (param instanceof Integer) {
                statement.setInt(i, (Integer)param);
            }
            else if (param instanceof Float) {
                statement.setFloat(i, (Float)param);
            }
            else if (param instanceof Double) {
                statement.setDouble(i, (Double)param);
            }
            else{
                statement.setString(i,(String) param);	
            }				
        }
    }
    public static boolean updateDataWithPrepareStmnt(Connection conn, String sql, List<Object> parameters) {
		try {
			PreparedStatement statement = conn.prepareStatement(sql);
			setStmntParameters(parameters, statement);
			int numberOfRowsUpdated = statement.executeUpdate();
			System.out.println("rows updated: " + numberOfRowsUpdated);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public static boolean fetchDataWithPrepareStmnt(Connection conn, String sql, List<Object> parameters) {
		//String sql = "select count(*) from t_varchar where name = ?";
		try {
			PreparedStatement statement = conn.prepareStatement(sql);
			setStmntParameters(parameters, statement);
			ResultSet rs = statement.executeQuery();
			printRS4Test(rs);
            if (rs!=null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}