package com.truecool.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 10:22:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class SqlUtils {
// -------------------------- STATIC METHODS --------------------------

  public static Connection createConnection(String driver, String url) {
    Connection connection = null;
    try {
      Class.forName(driver).newInstance();
      connection = DriverManager.getConnection(url);
    }    catch (Exception e) {
      System.out.println("Unable to connectfor: " + driver + " - " + url);
      System.out.println("Error : ".concat(String.valueOf(String.valueOf(e.getMessage()))));
      connection = null;
    }
    return connection;
  }

  public static String buildInsertStatement(String tableName, Map dataMap) {
    boolean isFirst = true;
    String returnValue = String.valueOf(String.valueOf((new StringBuffer("INSERT INTO ")).append(tableName).append(" (")));
    String columnValues = "";
    Iterator iterator = dataMap.keySet().iterator();
    do {
      if (!iterator.hasNext()) {
        break;
      }
      String keyName = (String) iterator.next();
      if (isValidColumnName(keyName)) {
        returnValue = String.valueOf(returnValue) + String.valueOf(isFirst ? ((Object) (keyName)) : ((Object) (",".concat(String.valueOf(String.valueOf(keyName))))));
        Object keyValue = dataMap.get(keyName);
        if (keyValue != null) {
          keyValue = keyValue.toString();
        } else {
          keyValue = " ";
        }
        keyValue = StringUtils.replace((String) keyValue, "'", "", true);
        columnValues = String.valueOf(columnValues) + String.valueOf(isFirst ? ((Object) (String.valueOf(String.valueOf((new StringBuffer("'")).append(keyValue).append("'"))))) : ((Object) (String.valueOf(String.valueOf((new StringBuffer(", '")).append(keyValue).append("'"))))));
        if (isFirst) {
          isFirst = false;
        }
      }
    } while (true);
    returnValue = String.valueOf(String.valueOf((new StringBuffer(String.valueOf(String.valueOf(returnValue)))).append(") VALUES (").append(columnValues).append(")")));
    return returnValue;
  }

  public static boolean isValidColumnName(String name) {
    boolean returnValue = true;
    if (name.length() < 1) {
      returnValue = false;
    } else if (name.indexOf(13) == 0) {
      returnValue = false;
    } else if (name.equals("INT1")) {
      returnValue = false;
    } else if (name.equals("INT2")) {
      returnValue = false;
    } else {
      returnValue = !isValidInteger(name);
    }
    return returnValue;
  }

  public static boolean isValidInteger(String string) {
    boolean returnValue = true;
    int i;
    try {
      i = Integer.parseInt(string);
    }
    catch (NumberFormatException e) {
      returnValue = false;
    }
    return returnValue;
  }

}
