package org.werelate.util;

import java.sql.*;
import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 * Date: Apr 23, 2008
 */
public class DatabaseConnectionHelper
{
   private static Logger logger = Logger.getLogger("org.werelate.util");

   private static final int MAX_RETRIES = 5;
   private static final int WAIT_MILLIS = 2000;

   private String url;
   private String username;
   private String password;
   private Connection conn;

   public DatabaseConnectionHelper(String url, String username, String password) throws ClassNotFoundException, IllegalAccessException, InstantiationException
   {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      this.url = url;
      this.username = username;
      this.password = password;
      this.conn = null;
   }

   public void connect() throws SQLException
   {
      conn = DriverManager.getConnection(url, username, password);
      conn.setAutoCommit(true);
   }

   public void startTransaction() throws SQLException
   {
      conn.setAutoCommit(false);
   }

   public void endTransaction(boolean commit) throws SQLException
   {
      if (commit) {
         conn.commit();
      }
      else {
         conn.rollback();
      }
      conn.setAutoCommit(true);
   }

   public PreparedStatement preparedStatement(String sql) throws SQLException
   {
      return conn.prepareStatement(sql);
   }

   public void close()
   {
      try
      {
         if (conn != null) {
            conn.close();
         }
      } catch (SQLException e)
      {
         // ignore
      }

      conn = null;
   }
}
