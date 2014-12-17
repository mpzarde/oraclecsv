package com.truecool.sql;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 10:32:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenericSQLWriter extends GenericSQLConnection {

  public GenericSQLWriter(String driver, String url) {
    super(driver, url);
  }

  public boolean executeStatement(String sql) {
    boolean returnValue = true;
    Statement statement = null;
    
    if (getConnection() == null) {
      throw new IllegalStateException("Valid connection required.");
    }

    try {
      statement = getConnection().createStatement();
      statement.executeUpdate(sql);
    } catch (SQLException ex) {
      System.out.println("Unable to execute SQL: " + sql);
      System.out.println(ex.getMessage());
      returnValue = false;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        }
        catch (Exception e) {
          returnValue = false;
        }
      }
    }
    return returnValue;
  }

}
