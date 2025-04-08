import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.Types;
import java.sql.*;

public class MultiSimpleQuery {

  //创建数据库连接。
  public static Connection GetConnection(String username, String passwd, String port) {
    String driver = "org.postgresql.Driver";
    String sourceURL = "jdbc:postgresql://localhost:" + port + "/regression?loggerLevel=off&preferQueryMode=simple";
    Connection conn = null;
    try {
      //加载数据库驱动。
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    try {
      //创建数据库连接。
      conn = DriverManager.getConnection(sourceURL, username, passwd);
      System.out.println("Connection succeed!");
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return conn;
  }

  public static void Test2(Connection conn) {
    Statement stmt1 = null;
    try {
      stmt1 = conn.createStatement();
      stmt1.executeUpdate("set enable_query_parameterization=off;");
      stmt1.executeUpdate("set enable_iud_fusion=on;");

      String sql = "delete from msq2 where a=1; delete from msq2 where a=2";
      boolean ress = stmt1.execute(sql);
      while (ress) {
          ResultSet rs = stmt1.getResultSet();
          while (rs.next()) {
              System.out.println(rs.getString(1));
          }
          rs.close();
          ress = stmt1.getMoreResults();
      }
      stmt1.close();
    } catch (SQLException e) {
      if (stmt1 != null) {
        try {
          stmt1.close();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
      e.printStackTrace();
    }
  }

  public static void Test1(Connection conn) {
    Statement stmt1 = null;
    try {
      stmt1 = conn.createStatement();
      stmt1.executeUpdate("set enable_query_parameterization=on;");

      String sql = "insert into multi_simple_query1 values(1); insert into multi_simple_query1 values(2)";
      boolean ress = stmt1.execute(sql);
      while (ress) {
          ResultSet rs = stmt1.getResultSet();
          while (rs.next()) {
              System.out.println(rs.getString(1));
          }
          rs.close();
          ress = stmt1.getMoreResults();
      }
      stmt1.close();
    } catch (SQLException e) {
      if (stmt1 != null) {
        try {
          stmt1.close();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
      e.printStackTrace();
    }
  }

  /**
   * 主程序，逐步调用各静态方法。
   * @param args
  */
  public static void main(String[] args) {
    //创建数据库连接。
    Connection conn = GetConnection("multi_simple_query", "tom@1234", args[0]);

    Test1(conn);
    Test2(conn);
    try {
      conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

}