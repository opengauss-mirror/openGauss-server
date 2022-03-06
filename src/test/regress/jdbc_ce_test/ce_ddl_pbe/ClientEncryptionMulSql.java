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

public class ClientEncryptionMulSql {
    
    public static void main(String[] args) throws IOException {
        // prepare connect info
        String PORT = args[0];
        String serverAddress = "localhost";
        String databseName = "regression";
        String username = "test";
        String password = "Gauss@123";
        //创建连接对象
        Connection con = null;
        String jdbcConnectionString  = 
        String.format("jdbc:postgresql://%s:%s/%s?enable_ce=1&loggerLevel=trace&loggerFile=cek_error.log", 
                        serverAddress, PORT, databseName);
        String unce_jdbcConnectionString = 
        String.format("jdbc:postgresql://%s:%s/%s", 
                        serverAddress, PORT, databseName);

        try {
            con = DriverManager.getConnection(jdbcConnectionString, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // CMK CEK测试
        UtilTool.executeSql(con, "drop table if exists test111;");
        UtilTool.executeSql(con, "drop table if exists test11;");
        UtilTool.executeSql(con, "drop table if exists test21;");
        UtilTool.executeSql(con, "drop table if exists test21;");
        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS MulCMK1 CASCADE;");

        UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY MulCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = SM4);");
        UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY CEK_jdbc1 WITH VALUES (CLIENT_MASTER_KEY = MulCMK1, ALGORITHM  = SM4_sm3);");
        System.out.println("test encrypted_key key end------------------------------------------------------------------------\n    ");
        
        // 加密表插入，插入数据
        System.out.println("test encrypted_key table   start");
        System.out.println("create encrypted_key table  start------------------------------------------------------------------------\n    ");
        UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS MulCMK1 CASCADE;CREATE CLIENT MASTER KEY MulCMK1 WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = SM4);");
	System.out.println("drop create CEK_jdbc1 end");

	// 多SQL执行(), 预期失败，不支持多sql场景
        String createtab_t1 = "CREATE COLUMN ENCRYPTION KEY  CEK_jdbc1 WITH VALUES (CLIENT_MASTER_KEY = MulCMK1, ALGORITHM  = SM4_sm3);drop table if exists test11;CREATE TABLE test11 ("+
                              "	id_number int4 encrypted with (column_encryption_key = CEK_jdbc1, encryption_type = DETERMINISTIC),          "+
                              "	dtime  varchar2(20)  ,             "+
                              "	 name  text    encrypted with (column_encryption_key = CEK_jdbc1, encryption_type = DETERMINISTIC)  "+
                              ")with (orientation=row);" ; 			  
        UtilTool.executeSql(con,createtab_t1);	
        System.out.println("create encrypted_key table end------------------------------------------------------------------------\n    ");

	UtilTool.executeSql(con,"drop COLUMN ENCRYPTION KEY IF EXISTS CEK_jdbc1 CASCADE;");	
	UtilTool.executeSql(con,"CREATE COLUMN ENCRYPTION KEY  CEK_jdbc1 WITH VALUES (CLIENT_MASTER_KEY = MulCMK1, ALGORITHM  = SM4_sm3);");	
	UtilTool.executeSql(con,"CREATE TABLE test11 ( "+
                              "	id_number int4 encrypted with (column_encryption_key = CEK_jdbc1, encryption_type = DETERMINISTIC),          "+
                              "	dtime  varchar2(20)  ,             "+
                              "	 name  text    encrypted with (column_encryption_key = CEK_jdbc1, encryption_type = DETERMINISTIC)  "+
                              ")with (orientation=row);");	

	UtilTool.executeSql(con, "insert into test11 values(1, '18:00', 'hell1');");
	UtilTool.executeSql(con, "insert into test11 values(2, '18:00', 'hell2');");
	UtilTool.executeSql(con, "insert into test11 values(3, '18:00', 'hell3');");

	// 批量insert, 预期成功, JDBC内部实际是多SQL实现, 针对此种场景不进行限制
	String pre_sql = "insert into test11 values(?, ?, ?);";
        try {
            PreparedStatement ps = con.prepareStatement(pre_sql);
	    ps.setInt(1, 50);
            ps.setString(2, "19:00");
            ps.setString(3, "hello");
	    ps.addBatch();

	    ps.setInt(1, 51);
            ps.setString(2, "20:00");
            ps.setString(3, "hello2");
	    ps.addBatch();

	    int[] actual = ps.executeBatch();
            //int  rs1 = ps.executeUpdate();
	} catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}

