package com.truecool.sql;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 10:25:50 AM
 */
public class GenericSQLReader extends GenericSQLConnection {
  public GenericSQLReader(Connection connection) {
    super(connection);
  }

  public GenericSQLReader(String driver, String url) {
    super(driver, url);
  }

  public List getData(String sql) {
    List dataList = null;
    ResultSet resultSet = null;
    Statement statement = null;

    if (getConnection() == null) {
      throw new IllegalStateException("Valid connection required.");
    }

    try {
      statement = getConnection().createStatement();
      if (statement != null) {
        resultSet = statement.executeQuery(sql);

        if (resultSet != null) {
          dataList = new ArrayList();
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          int iNumCols = resultSetMetaData.getColumnCount();
          HashMap dataMap;
          for (; resultSet.next(); dataList.add(dataMap)) {
            dataMap = new HashMap();
            for (int index = 1; index <= iNumCols; index++) {
              String columnName = resultSetMetaData.getColumnName(index);
              Object columnValue = resultSet.getObject(index);
              dataMap.put(columnName, columnValue);
            }
          }
        }
      }

    } catch (SQLException ex) {
      System.out.println("Unable to execute SQL: " + sql);
      System.out.println(ex.getMessage());
      dataList = null;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (Exception exception1) {
        }
      }
    }
    return dataList;
  }
}
