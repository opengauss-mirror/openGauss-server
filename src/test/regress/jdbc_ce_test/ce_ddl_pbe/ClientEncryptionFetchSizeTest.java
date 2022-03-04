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

public class ClientEncryptionFetchSizeTest {
    public static void testSqlByPassFetchSize(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS sqlbypassfetchsize_tab;");
        stmt.execute("CREATE TABLE sqlbypassfetchsize_tab(id int, name varchar(50) encrypted with (column_encryption_key = FetchSizeCEK, encryption_type = DETERMINISTIC), birthday timestamp );");

        PreparedStatement pstm = conn.prepareStatement("INSERT INTO sqlbypassfetchsize_tab values(?,?,?)");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            pstm.setInt(1, 1);
            pstm.setString(2, "name" + i);
            pstm.setTimestamp(3, new Timestamp(time));
            pstm.addBatch();
        }
        pstm.executeBatch();

        stmt.execute("set enable_opfusion=on;");
        conn.setAutoCommit(false);
        PreparedStatement pstm1 = conn.prepareStatement("select * from sqlbypassfetchsize_tab where id = ? and birthday = ?");
        pstm1.setInt(1, 1);
        pstm1.setTimestamp(2, new Timestamp(time));
        pstm1.setFetchSize(10);
        ResultSet rs = pstm1.executeQuery();
        while (rs.next()) {
            System.out.println("id: " + rs.getInt(1)+ " name: "+rs.getString(2)+" birthday: " + rs.getTimestamp(3));
        }
        stmt.execute("DROP TABLE IF EXISTS sqlbypassfetchsize_tab;");
        conn.commit();
        rs.close();
        pstm1.close();
        pstm.close();
        stmt.close();
        System.out.println("OK (testSqlByPassFetchSize test)");
    }

    public static void testFetchSize(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS fetchsize_tab;");
        stmt.execute("CREATE TABLE fetchsize_tab(id int, name varchar(50) encrypted with (column_encryption_key = FetchSizeCEK, encryption_type = DETERMINISTIC), birthday timestamp );");

        PreparedStatement pstm = conn.prepareStatement("INSERT INTO fetchsize_tab values(?,?,?);");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 50; i++) {
            pstm.setInt(1, 1);
            pstm.setString(2, "name" + i);
            pstm.setTimestamp(3, new Timestamp(time));
            pstm.addBatch();
        }
        pstm.executeBatch();

        conn.setAutoCommit(false);
        PreparedStatement pstm1 = conn.prepareStatement("select * from fetchsize_tab where id = ? and birthday = ?");
        pstm1.setInt(1, 1);
        pstm1.setTimestamp(2, new Timestamp(time));
        pstm1.setFetchSize(10);
        ResultSet rs = pstm1.executeQuery();
        while (rs.next()) {
            System.out.println("id: " + rs.getInt(1)+ " name: "+rs.getString(2)+" birthday: " + rs.getTimestamp(3));
        }
        stmt.execute("DROP TABLE IF EXISTS fetchsize_tab;");
        conn.commit();
        conn.setAutoCommit(true);
        rs.close();
        pstm1.close();
        pstm.close();
        stmt.close();
        System.out.println("OK (testFetchSize test)");
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
            UtilTool.executeSql(con, "DROP TABLE IF EXISTS fetchsize_tab;");
            UtilTool.executeSql(con, "DROP TABLE IF EXISTS sqlbypassfetchsize_tab;");
            UtilTool.executeSql(con, "DROP CLIENT MASTER KEY IF EXISTS FetchSizeCMK CASCADE;");
            UtilTool.executeSql(con, "CREATE CLIENT MASTER KEY FetchSizeCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = \"gs_ktool/1\" , ALGORITHM = AES_256_CBC);");
            UtilTool.executeSql(con, "CREATE COLUMN ENCRYPTION KEY FetchSizeCEK WITH VALUES (CLIENT_MASTER_KEY = FetchSizeCMK, ALGORITHM  = AEAD_AES_256_CBC_HMAC_SHA256);");
            testFetchSize(con);
            testSqlByPassFetchSize(con);
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













    