package com.truecool.ant;

import com.truecool.sql.GenericSQLReader;
import oracle.jdbc.OracleDriver;
import oracle.sql.CLOB;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 8:23:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class OracleExportTask extends Task {
  private String _dbmsuri;
  private String _table;
  private String _filepath;
  private String _dateformat;

  public String getDbmsuri() {
    return _dbmsuri;
  }

  public void setDbmsuri(String dbmsuri) {
    _dbmsuri = dbmsuri;
  }

  public String getTable() {
    return _table;
  }

  public void setTable(String table) {
    _table = table;
  }

  public String getFilepath() {
    return _filepath;
  }

  public void setFilepath(String filepath) {
    _filepath = filepath;
  }

  public String getDateformat() {
    return _dateformat;
  }

  public void setDateformat(String dateformat) {
    _dateformat = dateformat;
  }

  public void execute() throws BuildException {
    Connection connection = null;

    try {
      connection = setupConnection();
      exportData(connection, _table, _filepath);
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

  protected void exportData(Connection connection, String table, String filePath) throws Exception {
    BufferedWriter writer = null;
    ResultSetMetaData metaData = null;

    try {
      GenericSQLReader reader = new GenericSQLReader(connection);

      List data = reader.getData("SELECT * FROM " + table);

      if (data != null && data.size() > 0) {
        metaData = reader.getTableMetaData(table);
        int columnCount = metaData.getColumnCount();

        writer = new BufferedWriter(new FileWriter(filePath));

        for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
          Map row = (Map) data.get(rowIndex);

          for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            String colName = metaData.getColumnName(colIndex + 1);
            Object value = row.get(colName);

            if (value instanceof CLOB) {
              value = handleClob((CLOB) value, rowIndex, colIndex);
            } else if (value instanceof Timestamp) {
              value = handleTimeStamp((Timestamp) value);
            }

            writer.write((value == null ? "NULL" : value.toString()));
            writer.write((colIndex < columnCount - 1 ? "," : ""));
          }

          writer.write("\n");
        }
      }
    } finally {
      if (writer != null) {
        writer.flush();
        writer.close();
      }
    }
  }

  protected String handleClob(CLOB value, int rowIndex, int colIndex) throws Exception {
    String colValue = "clobdata-r" + rowIndex + "c" + colIndex + ".txt";
    File clobDataFile = new File(colValue);

    InputStream in = null;
    OutputStream out = null;

    try {
      byte buffer[] = new byte[(int) value.length()];
      in = value.getAsciiStream();
      in.read(buffer);
      String clobData = new String(buffer);

      out = new FileOutputStream(clobDataFile);
      out.write(clobData.getBytes());
    } finally {
      if (in != null) {
        in.close();
      }

      if (out != null) {
        out.flush();
        out.close();
      }

    }

    return "CLOB=" + colValue;
  }

  protected String handleTimeStamp(Timestamp date) {
    String formattedDate = null;
    SimpleDateFormat dateFormat = new SimpleDateFormat(getDateformat());
    formattedDate = dateFormat.format(new java.util.Date(date.getTime()));
    return formattedDate;
  }

}
