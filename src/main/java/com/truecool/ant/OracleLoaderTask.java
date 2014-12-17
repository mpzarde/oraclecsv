package com.truecool.ant;

import com.truecool.utils.StringUtils;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OraclePreparedStatement;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import java.io.*;
import java.sql.*;
import java.util.StringTokenizer;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Jan 29, 2006
 * Time: 5:00:13 PM
 */
public class OracleLoaderTask extends Task {
  private String _dbmsuri;
  private String _table;
  private boolean _truncate;
  private String _filepath;
  private String _dateformat;

  public String getDbmsuri() {
    return _dbmsuri;
  }

  public void setDbmsuri(String dbmsuri) {
    _dbmsuri = dbmsuri;
  }

  public String getDateformat() {
    return _dateformat;
  }

  public void setDateformat(String dateformat) {
    _dateformat = dateformat;
  }

  public void setTable(String table) {
    _table = table;
  }

  public void setTruncate(boolean truncate) {
    _truncate = truncate;
  }

  public void setFilepath(String filepath) {
    _filepath = filepath;
  }

  public void execute() throws BuildException {
    Connection connection = null;

    try {
      connection = setupConnection();
      loadData(connection, _table, _filepath, _truncate);
      connection.commit();
    } catch (Exception e) {
      throw new BuildException(e);
    } finally {
      cleanupConnection(connection);
    }
  }

  protected Connection setupConnection() throws ClassNotFoundException, SQLException {
    DriverManager.registerDriver(new OracleDriver());
    return DriverManager.getConnection(_dbmsuri);
  }

  protected void cleanupConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new BuildException(e);
      }
    }
  }

  protected void loadData(Connection connection, String table, String filePath, boolean truncate) throws Exception {
    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new FileReader(filePath));

      if (truncate) {
        String sql = "TRUNCATE TABLE " + table;
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();
      }

      String line = null;
      while ((line = reader.readLine()) != null) {
        handleLine(connection, table, line);
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  protected void handleLine(Connection connection, String table, String line) throws Exception {
    String sql = prepareSql(connection, table, line);
    PreparedStatement statement = prepareStatement(connection, sql, line);
    statement.execute();
    statement.close();
  }

  protected PreparedStatement prepareStatement(Connection connection, String sql, String line) throws Exception {
    OraclePreparedStatement statement = (OraclePreparedStatement) connection.prepareStatement(sql);

    StringTokenizer tokenizer = new StringTokenizer(line, ",");

    int index = 1;
    while (tokenizer.hasMoreElements()) {
      String element = (String) tokenizer.nextElement();

      if (StringUtils.isValidInteger(element)) {
        statement.setInt(index, Integer.parseInt(element));
      } else if (StringUtils.isValidClob(element)) {
        String fileName = element.substring(5);
        File file = new File(fileName);
        InputStream fis = new FileInputStream(file);
        statement.setAsciiStream(index, fis, (int) file.length());
      } else if (StringUtils.isValidDate(element)) {
        java.util.Date date = StringUtils.getValidDate(element, getDateformat());
        Timestamp timestamp = new Timestamp(date.getTime());
        statement.setTimestamp(index, timestamp);
      } else {
        statement.setString(index, element);
      }

      index++;
    }

    return statement;
  }


  protected String prepareSql(Connection connection, String table, String line) throws Exception {
    PreparedStatement statement = null;
    ResultSetMetaData metaData = null;
    String metaDataSql = "SELECT * FROM " + table + " WHERE 1 = 2";

    statement = connection.prepareStatement(metaDataSql);
    statement.execute();
    metaData = statement.getMetaData();


    String sql = "INSERT INTO " + table + "(";

    for (int index = 1; index <= metaData.getColumnCount(); index ++) {
      String colName = metaData.getColumnName(index);

      sql += colName;

      if (index < metaData.getColumnCount()) {
        sql += ",";
      }
    }

    statement.close();

    sql += ") VALUES (";

    StringTokenizer tokenizer = new StringTokenizer(line, ",");

    while (tokenizer.hasMoreElements()) {
      tokenizer.nextElement();
      sql += "?";

      if (tokenizer.hasMoreElements()) {
        sql += ",";
      }
    }

    sql += ")";

    return sql;
  }

}
