package com.truecool.sql;

import com.truecool.utils.SqlUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 10:21:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenericSQLConnection {
// ------------------------------ FIELDS ------------------------------
  private String _driver;
  private String _url;
  private Connection _connection;

// --------------------------- CONSTRUCTORS ---------------------------

  public GenericSQLConnection(Connection connection) {
    setConnection(connection);
  }

  public GenericSQLConnection(String driver, String url) {
    setDriver(driver);
    setUrl(url);
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public Connection getConnection() {
    if (_connection == null) {
      open();
    }

    return _connection;
  }

  public void setConnection(Connection connection) {
    _connection = connection;
  }

  public String getDriver() {
    return _driver;
  }

  public void setDriver(String driver) {
    _driver = driver;
  }

  public String getUrl() {
    return _url;
  }

  public void setUrl(String url) {
    _url = url;
  }

// -------------------------- OTHER METHODS --------------------------

  public ResultSetMetaData getTableMetaData(String tableName) throws Exception {
    ResultSetMetaData metaData = null;

    if (getConnection() == null) {
      throw new IllegalStateException("Valid connection required.");
    } else {
      String metaDataSql = "SELECT * FROM " + tableName + " WHERE 1 = 2";
      PreparedStatement statement = getConnection().prepareStatement(metaDataSql);
      statement.execute();
      metaData = statement.getMetaData();
    }

    return metaData;

  }

  public void commit() throws SQLException {
    if (_connection != null) {
      _connection.commit();
    }
  }

  public void open() {
    Connection connection = SqlUtils.createConnection(getDriver(), getUrl());

    if (connection == null) {
      throw new IllegalStateException("Unable to make specified connection.");
    } else {
      setConnection(connection);
    }
  }

  public void close() throws SQLException {
    if (_connection != null) {
      _connection.close();
    }
  }
}
