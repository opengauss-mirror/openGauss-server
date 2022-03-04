package gauss.regress.jdbc.bintests;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class ResultSetFetchNoCLBin implements IBinaryTest {

	@Override
	public void execute(DatabaseConnection4Test conn) {
		
		String sql;
		sql = "CREATE TABLE IF NOT EXISTS t_varchar(id INT, name varchar(50), address varchar(50));";
		conn.executeSql(sql);
		//Create records
		for (int i = 0; i < 65; ++i) {
			sql = "INSERT INTO t_varchar (id, name, address) VALUES (" + i + ", 'MyName', 'MyAddress')";
			conn.executeSql(sql);			
		}
		try {
			conn.getConnection().setAutoCommit(false);//Auto commit must be off for this to happen
			Statement st = conn.getConnection().createStatement();
			st.setFetchSize(10);//set the client fetch size to 10
			ResultSet rs = st.executeQuery("select * from t_varchar order by id");
			conn.printRS4Test(rs);//loop thru the entire data set (should perform initial call & 6 fetch
			st.close();
		} catch (SQLException e) {		
			e.printStackTrace();
		}
		conn.executeSql("DROP table t_varchar");		
		conn.fetchData("select * from gs_column_keys");
	}

}
