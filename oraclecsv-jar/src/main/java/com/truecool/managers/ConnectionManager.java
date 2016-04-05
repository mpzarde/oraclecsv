package com.truecool.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.StringTokenizer;

//import oracle.jdbc.OraclePreparedStatement;



import org.apache.tools.ant.BuildException;

import com.truecool.utils.StringUtils;

public class ConnectionManager {

	private static Connection connection = null;
	
	private static String[] colTypes = null;

	public static void setupConnection(Driver driver, String url) throws ClassNotFoundException, SQLException {
		
		DriverManager.registerDriver(driver);
		connection = DriverManager.getConnection(url);
	}

	public static void cleanupConnection() {
		
		if (getConnection() != null) {
			try {
				if( getConnection().getAutoCommit() == false){
					getConnection().commit();
				}
				getConnection().close();
			} catch (SQLException e) {
				throw new BuildException(e);
			}
		}
	}

	public static Connection getConnection() {
		return connection;
	}

	private static String prepareSql( String table, String line) throws Exception {
		PreparedStatement statement = null;
		ResultSetMetaData metaData = null;
		String metaDataSql = "SELECT * FROM " + table + " WHERE 1 = 2";

		statement = getConnection().prepareStatement(metaDataSql);
		statement.execute();
		metaData = statement.getMetaData();


		String sql = "INSERT INTO " + table + "(";
		
		int size = metaData.getColumnCount();
		colTypes = new String[size];
		
		for (int index = 1; index <= size; index ++) {
			String colName = metaData.getColumnName(index);
			colTypes[index-1] = metaData.getColumnClassName(index);
			
			sql += colName;

			if (index < metaData.getColumnCount()) {
				sql += ",";
			}
		}
//		System.out.println(Arrays.toString(colTypes));

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

	public static void handleLine( String table, String line,String dateformat) throws Exception {
		String sql = prepareSql( table, line);
		PreparedStatement statement = prepareStatement( sql, line,dateformat);
		//statement.execute();
		statement.cancel();
		statement.close();
	}

	private static PreparedStatement prepareStatement( String sql, String line, String dateformat) throws Exception {
		PreparedStatement statement = connection.prepareStatement(sql);

		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		int index = 1;
		while (tokenizer.hasMoreElements()) {
			String element = (String) tokenizer.nextElement();
			element = StringUtils.decodeString(element);
			
			String type = colTypes[index-1];
//			System.out.println(type + " - " + element );
			
			if( type.equals("java.math.BigDecimal") ) {
				statement.setInt(index, Integer.parseInt(element));
			} 
			else if ( type.equals("java.lang.String") ) {
				statement.setString(index, element);
			}
			else if ( type.equals("oracle.sql.TIMESTAMP")) {
				if(element!=null && !element.equalsIgnoreCase("null")){
					java.util.Date date = StringUtils.getValidDate(element, dateformat);
					Timestamp timestamp = new Timestamp(date.getTime());
					statement.setTimestamp(index, timestamp);
				}
				else{
					statement.setTimestamp(index, null);
				}
					
			} 
			else if ( type.equals("oracle.jdbc.OracleClob") ) {
				if(element!=null && !element.equalsIgnoreCase("null")){
				String fileName = element.substring(5);
				File file = new File(fileName);
				file.deleteOnExit();
				InputStream fis = new FileInputStream(file);
				statement.setAsciiStream(index, fis, (int) file.length());
				}
				else{
					statement.setAsciiStream(index, null, 0);
				}
			}
			else{
				statement.setString(index, element);
			}

			index++;
		}

		return statement;
	}

	public static void performTruncate(String table ) throws SQLException {
		String sql = "TRUNCATE TABLE " + table;
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
	}
}
