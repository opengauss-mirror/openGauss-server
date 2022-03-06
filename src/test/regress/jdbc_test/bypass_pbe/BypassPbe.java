import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import org.postgresql.util.*;

public
class BypassPbe {
public
    static Connection GetConnection(String port)
    {
        String urls = "jdbc:postgresql://localhost:" + port + "/regression?prepareThreshold=0&loggerLevel=off";
        String driver = "org.postgresql.Driver";

        Properties urlProps = new Properties();
        urlProps.setProperty("user", "tom");
        urlProps.setProperty("password", "tom@1234");

        Connection conn = null;
        try {
            Class.forName(driver).newInstance();
            conn = DriverManager.getConnection(urls, urlProps);
            System.out.println("Connection succeed!");
        } catch (Exception exception) {
            exception.printStackTrace();
            return null;
        }

        return conn;
    };


public
    static void CreateTable(Connection conn)
    {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();

            int drc = stmt.executeUpdate("drop table if exists jdbcpbebypass ;");

            int rc = stmt.executeUpdate("create table jdbcpbebypass(id int, class int, name text, score float);");

            stmt.executeUpdate("create index on jdbcpbebypass(class);");

            stmt.close();
        } catch (SQLException exception) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }

public
    static void InsertData(Connection conn)
    {
        Statement stmt = null;
        PreparedStatement pstmt = null;
        try {
            String insertSql = "insert into jdbcpbebypass(id, class, name, score ) values (?,?,?,?);";

            pstmt = conn.prepareStatement(insertSql);
            for (int j = 0; j <= 6; j++) {
                for (int i = 1; i <= 10; i++) {
                    pstmt.setInt(1, j * 10 + i);
                    pstmt.setInt(2, j);
                    pstmt.setString(3, "name" + (j * 10 + i));
                    pstmt.setFloat(4, (float)Math.random() * 100 + 1);
                    int ans = pstmt.executeUpdate();
                }
            }
            pstmt.close();
        } catch (PSQLException exception2) {
            if (pstmt != null) {
                try {
                    System.out.println("insert again");
                    pstmt.executeUpdate();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            System.out.println("over");
            exception2.printStackTrace();
        } catch (SQLException exception) {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }

public
    static void SelectData(Connection conn)
    {
        setFusion(conn);

        Statement stmt = null;
        PreparedStatement pstmt = null;
        PreparedStatement pstmt2 = null;
        PreparedStatement pstmt3 = null;
        try {
            String selectSql = "select name from jdbcpbebypass where class=?;";
            String selectSql2 = "select id from jdbcpbebypass where class=?;";
            String selectSql3 = "select name from jdbcpbebypass where class=? offset 1 limit 10;";

            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(selectSql);
            pstmt2 = conn.prepareStatement(selectSql2);
            pstmt3 = conn.prepareStatement(selectSql2);
            pstmt.setFetchSize(3);
            pstmt2.setFetchSize(3);
            pstmt3.setFetchSize(2);
            pstmt.setInt(1, 1);

            ResultSet rs = pstmt.executeQuery(); // P1 B1
            int round = 0;
            while (rs.next()) { // E1 E1 分了两次取结果，每次最多取3条
                System.err.println("name=" + rs.getString(1));
                System.err.println();
                round++;
                if (round == 6)
                    break;
            }
            System.err.println("break of a resultset of pstmt1");

            pstmt2.setInt(1, 3);

            round = 0;
            ResultSet rs2 = pstmt2.executeQuery(); // P2 B2
            while (rs2.next()) {                   // E2E2
                System.err.println("id=" + rs2.getInt(1));
                System.err.println();
                round++;
                if (round == 6)
                    break;
            }
            System.err.println("break of a resultset of pstmt2");

            round = 0;
            System.err.println("start E1E1");
            while (rs.next()) { // E1E1
                System.err.println("name=" + rs.getString(1));
                System.err.println();
                round++;
                if (round == 4)
                    break;
            }
            System.err.println("end E1E1");

            round = 0;
            System.err.println("start E2E2");
            while (rs2.next()) { // E2E2
                System.err.println("id=" + rs2.getInt(1));
                System.err.println();
                round++;
                if (round == 4)
                    break;
            }
            System.err.println("end E2E2");
            System.err.println("start OFFSET 1 LIMIT 10 E2");
            round = 0;
            ResultSet rs3 = pstmt2.executeQuery();
            while (rs3.next()) {
                System.err.println("name=" + rs.getString(1));
                System.err.println();
                round++;
                if (round == 10)
                    break;
            }
            System.err.println("end OFFSET 1 LIMIT 10 E2");
            pstmt.close();
            pstmt2.close();
            pstmt3.close();
        } catch (SQLException exception) {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }

public
    static void UpdateData(Connection conn)
    {
        setFusion(conn);

        PreparedStatement pstmt = null;
        try {
            String selectSql = "update jdbcpbebypass set name='name_k' where class=?;";

            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(selectSql);
            pstmt.setFetchSize(3);
            pstmt.setInt(1, 1);

            int aff_row = pstmt.executeUpdate();
            System.err.println("aff_row=" + aff_row);

            pstmt.close();
        } catch (SQLException exception) {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }


public
    static void SelectDataDirectly(Connection conn)
    {
        setFusion(conn);
        Statement stmt = null;

        try {
            stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery("select * from jdbcpbebypass;"); // P B E
            while (rs.next()) {
                System.err.println("id=" + rs.getInt(1) + ",class=" + rs.getInt(2) + ",name=" + rs.getString(3));
            }
            stmt.close();
        } catch (SQLException exception) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }

    static void setFusion(Connection conn)
    {
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate("set enable_bitmapscan=off;");
            stmt.executeUpdate("set enable_seqscan=off;");
            stmt.executeUpdate("set enable_opfusion=on;");
        } catch (SQLException exception) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException exception1) {
                    exception1.printStackTrace();
                }
            }
            exception.printStackTrace();
        }
    }

public static void SelectDataDataRow(Connection conn) {
        Statement stmt = null;
        PreparedStatement pstmt = null;
        PreparedStatement pstmt2 = null;
        try{
             stmt = conn.createStatement();
             conn.setAutoCommit(false);
             int rc = stmt.executeUpdate("create table t1(c1 int primary key, c2 int, c3 int);");
             rc = stmt.executeUpdate("insert into t1 select i,i,i from generate_series(1,100) i;");
             rc = stmt.executeUpdate("set enable_bitmapscan =off;"); // P B E
             rc = stmt.executeUpdate("set enable_seqscan =off;"); // P B E
            String selectSql = "select * from t1 where c1>? limit 2;";
            pstmt = conn.prepareStatement(selectSql);
            pstmt.setMaxRows(2);
            pstmt.setInt(1,1);
            ResultSet rs = pstmt.executeQuery(); // P1 B1 E1
            int round =0;
            while(rs.next()){ //E1 E1 E1
                System.err.println("c2="+rs.getInt(1));
                System.err.println();
                round++;
            }
             conn.commit();
            System.err.println("break of a resultset of pstmt1");
            round = 0;
            pstmt.setMaxRows(2);
            pstmt.setInt(1,1);
            rs = pstmt.executeQuery();
            while(rs.next()) {
                System.err.println("c2="+rs.getInt(1));System.err.println();
            }
            System.err.println("break of a resultset of pstmt1");
           round = 0;
            pstmt.setMaxRows(2);
            pstmt.setInt(1,1);
            rs = pstmt.executeQuery();
            while(rs.next()) {
                System.err.println("c2="+rs.getInt(1));System.err.println();
            }
            System.err.println("break of a resultset of pstmt1");
           round = 0;
            while(rs.next()) { //
                System.err.println("c2="+rs.getInt(1));
                System.err.println();
                round++;
            }
            System.err.println("end of a resultset");
            rc = stmt.executeUpdate("drop table t1;");
            pstmt.close();
        } catch (SQLException e) {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }

            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }



public
    static void main(String[] args)
    {
        String PORT = args[0];
        Connection conn = GetConnection(PORT);

        if (conn == null) {
            System.out.println("connection failed");
            return;
        }
        CreateTable(conn);
        InsertData(conn);
        SelectData(conn);
        UpdateData(conn);
        SelectDataDirectly(conn);
		SelectDataDataRow(conn);
        try {
            conn.close();
            System.out.println("close connection");
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
