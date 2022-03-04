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
import java.sql.*;

public class ClientEncryptionTransactionTest {

    public static void testEnd(Connection con) throws SQLException {
        String[] sqls = {
                "begin",
                "insert into test_table values(1, '1')",
                "rollback",
                "begin",
                "insert into test_table values(2, '2')",
                "commit transaction",
                "begin",
                "insert into test_table values(3, '3')",
                "end",
                "begin",
                "insert into test_table values(4, '4')",
                "end",
                "begin",
                "insert into test_table values(5, '5')",
                "end",
                "begin",
                "insert into test_table values(6, '6')",
                "end transaction",
                "begin",
                "insert into test_table values(7, '7')",
                "rollback"
        };

        con.setAutoCommit(false);
        UtilTool.fetchData(con, "select * from test_table;");
        for(String sql : sqls) {
            UtilTool.executeSql(con, sql);
            System.out.println("sql--"+sql);
        }
        //another session
        PreparedStatement pstmt = con.prepareStatement("select * from test_table order by id");
        ResultSet resultSet = pstmt.executeQuery();
        while(resultSet.next()) {
            System.out.println(resultSet.getInt(1) + " " +resultSet.getString(2));
        }
        pstmt.close();
        System.out.println("OK testEnd test");
    }

    public static void testRollback(Connection con) throws SQLException {
        String[] sqls = {
                "drop table if exists test_table;",
                "create table test_table(id int, name varchar2(20) encrypted with (column_encryption_key = TransactionCEK, encryption_type = DETERMINISTIC));",
                "insert into test_table values(1,'x');",
                "insert into test_table values(11,'xx');",
                "insert into test_table values(111,'xxx');",
                "insert into test_table values(1,'xca');",
                "begin;",
                "insert into test_table values(1,'abc');",
                "rollback;",
                "begin work;",
                "insert into test_table values(1,'def');",
                "commit;"
        };
        for(String sql : sqls) {
            UtilTool.executeSql(con, sql);
        }
        UtilTool.fetchData(con, "select * from test_table;");
        UtilTool.executeSql(con, "truncate table test_table;");
        UtilTool.fetchData(con, "select * from test_table;");
        System.out.println("OK testRollback test");
    }

    public static void main(String[] args) throws IOException {
        // prepare connect info
	    String PORT = args[0];
        String urls = "jdbc:postgresql://localhost:"+ PORT +"/regression?enable_ce=1";
        Properties urlProps = new Properties();

        // create connection
        Connection con = null;
        try {
            urlProps.setProperty("user", "test");
            urlProps.setProperty("password", "Gauss@123");
            con = DriverManager.getConnection(urls, urlProps);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }

        try {
            UtilTool.executeSql(con, "DROP TABLE IF EXISTS test_table;");

            UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS TransactionCMK CASCADE;");
            UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY TransactionCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY TransactionCEK WITH VALUES (CLIENT_MASTER_KEY = TransactionCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");
            testRollback(con);
            testEnd(con);
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}