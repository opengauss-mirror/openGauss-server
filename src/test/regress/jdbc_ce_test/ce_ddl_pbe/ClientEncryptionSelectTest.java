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

public class ClientEncryptionSelectTest {
    public static void main(String[] args) throws IOException {
        // prepare connect info
        String PORT = args[0];
        String serverAddress = "localhost";
        String databseName = "regression";
        String username = "test";
        String password = "Gauss@123";
        Connection con = null;
		String jdbcConnectionString  = 
				String.format("jdbc:postgresql://%s:%s/%s?enable_ce=1", 
						serverAddress, PORT, databseName);
		try {
			con = DriverManager.getConnection(jdbcConnectionString, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
            System.exit(0);
		}
        UtilTool.executeSql(con, "drop table if exists creditcard_info;");
        UtilTool.executeSql(con, "drop table if exists creditcard_info1;");
        UtilTool.executeSql(con, "drop table if exists creditcard_info2;");
        UtilTool.executeSql(con, "drop table if exists creditcard_info3;");
        UtilTool.executeSql(con, "drop table if exists creditcard_info2_1;");
        UtilTool.executeSql(con, "drop table if exists creditcard_info3_1;");
		UtilTool.executeSql(con, "drop table if exists un_encrypted_table;");
		UtilTool.executeSql(con, "drop table if exists batch_table;");
        UtilTool.executeSql(con, "DROP TABLE IF  EXISTS table_random;");

		// create un_encrypted table
		UtilTool.executeSql(con,"CREATE TABLE un_encrypted_table(id_number    int, name  varchar(50), credit_card  varchar(19));");
		UtilTool.executeSql(con, "INSERT INTO un_encrypted_table VALUES (1,'joe','6217986500001288393');");
		UtilTool.executeSql(con, "INSERT INTO  un_encrypted_table VALUES (2, 'joy','6219985678349800033');");
		UtilTool.fetchData(con, "select * from un_encrypted_table;");
		UtilTool.executeSql(con, "ALTER TABLE un_encrypted_table ALTER COLUMN name SET DEFAULT 'gauss';");
		UtilTool.executeSql(con, "INSERT INTO  un_encrypted_table(id_number, credit_card) VALUES (3,'6219985678349800033');");
		UtilTool.fetchData(con, "select * from un_encrypted_table;");


        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS ImgCMK1 CASCADE;");
        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS ImgCMK CASCADE;");
        UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY ImgCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);");
        UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY ImgCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/2\" , ALGORITHM = AES_256_CBC);");
        UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY ImgCEK1 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK1, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");
        UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY ImgCEK WITH VALUES (CLIENT_MASTER_KEY = ImgCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");

        UtilTool.executeSql(con,"CREATE TABLE creditcard_info (id_number    int, name  varchar(50) encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC),"+
        "credit_card  varchar(19) encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (1,'joe','6217986500001288393');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (2, 'joy','6219985678349800033');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (3, 'xiaoli', '6211877800001008888');");
        List<Object> parameters = new ArrayList<>();
        parameters.add(4);
        parameters.add("Nina");
        parameters.add("6189486985800056893");
        UtilTool.updateDataWithPrepareStmnt(con,"INSERT INTO creditcard_info VALUES (?, ?, ?);",parameters);
        System.out.println("update--------------------");
        List<Object> parameters_up = new ArrayList<>();
        parameters_up.add("Nina1");
        parameters_up.add(4);
        UtilTool.updateDataWithPrepareStmnt(con,"update creditcard_info set name = ? where id_number = ?;",parameters_up);

        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (5, 'fanny', '7689458639568569354');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (6, 'cora', '7584572945579384675');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info VALUES (7, 'nancy', '7497593456879650677');");
        System.out.println("select * from creditcard_info where name = 'Nina';");
        UtilTool.fetchData(con, "select * from creditcard_info where name = 'Nina';");

         System.out.println("delete--------------------");
        List<Object> parameters_de = new ArrayList<>();
        parameters_de.add(5);
        parameters_de.add("fanny");
        UtilTool.updateDataWithPrepareStmnt(con,"delete from creditcard_info where id_number = ? and name = ?;",parameters_de);
        System.out.println("select * from creditcard_info;");
        UtilTool.fetchData(con, "select * from creditcard_info;");

        // 加上就有问题
        List<Object> select_parameters = new ArrayList<>();
        select_parameters.add("joe");
        select_parameters.add("6217986500001288393");
        UtilTool.fetchDataWithPrepareStmnt(con, "SELECT * from creditcard_info where name = ? and credit_card = ?;", select_parameters);

        UtilTool.executeSql(con, "CREATE TABLE creditcard_info1 (id_number    int, name         text encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC),"+
        "credit_card  varchar(19) encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info1 VALUES (1,'joe','6217986500001288393');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info1 VALUES (2, 'joy','6219985678349800033');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info1 VALUES (3, 'xiaoli', '6211877800001008888');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info1 VALUES (4, 'Nina', '6189486985800056893');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info1 VALUES (5, 'fanny', '7689458639568569354');");
        System.out.println("select * from creditcard_info1 where name = (select name from creditcard_info order by id_number limit 1);");
        UtilTool.fetchData(con, "select * from creditcard_info1 where name = (select name from creditcard_info order by id_number limit 1);");
        
        UtilTool.executeSql(con, "CREATE TABLE creditcard_info2 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "credit_card  varchar(19) encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info2 VALUES (1,'joe','joe','6217986500001288393');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info2 VALUES (2, 'joy','joy','6219985678349800033');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info2 VALUES (3, 'xiaoli','xiaoli', '6211877800001008888');");

        UtilTool.executeSql(con, "CREATE TABLE creditcard_info3 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC),"+
        "credit_card  int encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3 VALUES (1,'joe','joe',62176500);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3 VALUES (2, 'joy','joy',62199856);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3 VALUES (3, 'xiaoli','xiaoli', 621187780);");
        System.out.println("select * from creditcard_info2 where name1 = (select name1 from creditcard_info3 order by id_number limit 1);");
        UtilTool.fetchData(con, "select * from creditcard_info2 where name1 = (select name1 from creditcard_info3 order by id_number limit 1);");
        UtilTool.fetchData(con, "select * from (select * from creditcard_info3) where credit_card = 62176500;");
        UtilTool.fetchData(con, "select name2 from (select * from creditcard_info3) group by name1 ,name2 having name1 = 'joe';");
        UtilTool.fetchData(con, "select * from (select * from creditcard_info3 where credit_card = 62176500);");
        UtilTool.fetchData(con, "select * from (select * from creditcard_info3) as a , (select * from creditcard_info2) as b where a.credit_card = 62176500 and a.name1='joe' and b.name1='joe';");
        UtilTool.fetchData(con, "select credit_card, name1  from (select name1,credit_card from creditcard_info3) as a ,  (select name2 from creditcard_info2) as b  where name1='joe' and name2='joe' group by credit_card, name1 having credit_card = 62176500;");

        UtilTool.executeSql(con, "CREATE TABLE creditcard_info2_1 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = RANDOMIZED),"+
        "credit_card  varchar(19) encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");

        UtilTool.executeSql(con, "INSERT INTO creditcard_info2_1 VALUES (1,'joe','joe','6217986500001288393');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info2_1 VALUES (2, 'joy','joy','6219985678349800033');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info2_1 VALUES (3, 'xiaoli','xiaoli', '6211877800001008888');");

        UtilTool.executeSql(con, "CREATE TABLE creditcard_info3_1 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC),"+
        "credit_card  int encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3_1 VALUES (1,'joe','joe',62176500);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3_1 VALUES (2, 'joy','joy',62199856);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info3_1 VALUES (3, 'xiaoli','xiaoli', 621187780);");
        UtilTool.fetchData(con, "select name1 from  creditcard_info2  INTERSECT select name2 from  creditcard_info2;");
        UtilTool.fetchData(con, "select name1 from  creditcard_info3  UNION select name2 from  creditcard_info2;");
		System.out.println("select name2 from  creditcard_info3  INTERSECT select name2 from  creditcard_info2;");
        UtilTool.fetchData(con, "select name2 from  creditcard_info3  INTERSECT select name2 from  creditcard_info2;");

        UtilTool.executeSql(con, "CREATE TEMP TABLE creditcard_info4 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = RANDOMIZED),"+
        "credit_card  varchar(19) encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");

        UtilTool.executeSql(con, "INSERT INTO creditcard_info4 VALUES (1,'joe','joe','6217986500001288393');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info4 VALUES (2, 'joy','joy','6219985678349800033');");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info4 VALUES (3, 'xiaoli','xiaoli', '6211877800001008888');");

        UtilTool.executeSql(con, "CREATE TEMP TABLE creditcard_info5 (id_number    int, name1  text encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC),"+
        "name2  text encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC),"+
        "credit_card  int encrypted with (column_encryption_key = ImgCEK1, encryption_type = DETERMINISTIC));");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info5 VALUES (1,'joe','joe',62176500);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info5 VALUES (2, 'joy','joy',62199856);");
        UtilTool.executeSql(con, "INSERT INTO creditcard_info5 VALUES (3, 'xiaoli','xiaoli', 621187780);");
        
        UtilTool.fetchData(con, "select * from creditcard_info4 where name1 = (select name1 from creditcard_info5 order by id_number limit 1);");
        UtilTool.fetchData(con, "select * from (select * from creditcard_info5) where credit_card = 62176500;");
		System.out.println("select name2 from (select * from creditcard_info5) group by name1 ,name2 having name1 = 'joe';");
        UtilTool.fetchData(con, "select name2 from (select * from creditcard_info5) group by name1 ,name2 having name1 = 'joe';");
		System.out.println("select * from (select * from creditcard_info5 where credit_card = 62176500);");
        UtilTool.fetchData(con, "select * from (select * from creditcard_info5 where credit_card = 62176500);");
        
		
		// batchpreparedstatement
		
        UtilTool.executeSql(con, "CREATE TABLE IF NOT EXISTS batch_table(id INT, name varchar(50) encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC), address varchar(50)  encrypted with (column_encryption_key = ImgCEK, encryption_type = DETERMINISTIC));");
		PreparedStatement statemnet = null;
		try {
			String sql = "INSERT INTO batch_table (id, name, address) VALUES (?,?,?)";
			System.out.println("starting batch : " + sql);
			statemnet = con.prepareStatement(sql);
			int loopCount = 20;
			System.out.println("Number of rows to add: " + loopCount);
			for (int i = 1; i < loopCount + 1; ++i) {
				statemnet.setInt(1, i);
				statemnet.setString(2, "Name " + i);
				statemnet.setString(3, "Address " + i);
                // Add row to the batch.
                statemnet.addBatch();
				
        }
			System.out.println("executing batch ...");
			statemnet.executeBatch();
			statemnet.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
        // test metadata
        String query = "Select * from batch_table order by id";
        try {
            Statement stmt_metadata = con.createStatement();
            ResultSet rs = stmt_metadata.executeQuery(query);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            for (int k = 1; k < resultSetMetaData.getColumnCount() + 1; ++k) {
                System.out.println("Index: " + k + " column name: " + resultSetMetaData.getColumnName(k));
                System.out.println(" getColumnDisplaySize is: " + resultSetMetaData.getColumnDisplaySize(k));
                System.out.println(" getColumnClassName is: " + resultSetMetaData.getColumnClassName(k));
                System.out.println(" getColumnLabel is: " + resultSetMetaData.getColumnLabel(k));
                System.out.println(" getColumnType is: " + resultSetMetaData.getColumnType(k));
                System.out.println(" getColumnTypeName is: " + resultSetMetaData.getColumnTypeName(k));
                System.out.println(" getPrecision is: " + resultSetMetaData.getPrecision(k));
                System.out.println(" getScale is: " + resultSetMetaData.getScale(k));
                System.out.println(" isNullable is: " + resultSetMetaData.isNullable(k));
                System.out.println(" isNullable is: " + resultSetMetaData.isAutoIncrement(k));
                System.out.println(" isCaseSensitive is: " + resultSetMetaData.isCaseSensitive(k));
                System.out.println(" isCurrency is: " + resultSetMetaData.isCurrency(k));
                System.out.println(" isReadOnly is: " + resultSetMetaData.isReadOnly(k));
                System.out.println(" isSigned is: " + resultSetMetaData.isSigned(k));
                System.out.println(" isWritable is: " + resultSetMetaData.isWritable(k));
                System.out.println(" isDefinitelyWritable is: " + resultSetMetaData.isDefinitelyWritable(k));
                System.out.println(" isSearchable is: " + resultSetMetaData.isSearchable(k));
                System.out.println(" ");
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            }
            rs.close();
            stmt_metadata.close();
        } catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
        UtilTool.executeSql(con, "DROP TABLE IF  EXISTS table_random;");
        UtilTool.executeSql(con, "CREATE TABLE IF NOT EXISTS table_random (i1 INT ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ImgCEK, ENCRYPTION_TYPE = RANDOMIZED) , i2 INT);");
        UtilTool.executeSql(con, "INSERT INTO table_random (i1, i2) VALUES (12, 13);");
        UtilTool.executeSql(con, "INSERT INTO table_random VALUES (15,16);");
        UtilTool.executeSql(con, "INSERT INTO table_random (i1, i2) VALUES (22, 23), (24, 25), (26,27);");
        UtilTool.executeSql(con, "INSERT INTO table_random VALUES (35,36), (36,37), (38,39);");
        UtilTool.fetchData(con, "SELECT * from table_random ORDER BY i2;");
        UtilTool.fetchData(con, "SELECT i1 FROM table_random WHERE i2  = 25;");
        UtilTool.fetchData(con, "SELECT i1 FROM table_random WHERE i1  = 24;");

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

