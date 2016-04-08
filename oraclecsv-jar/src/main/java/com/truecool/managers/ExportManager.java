package com.truecool.managers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Driver;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import oracle.sql.CLOB;
import oracle.sql.TIMESTAMP;

import com.truecool.sql.GenericSQLReader;
import com.truecool.utils.StringUtils;

public class ExportManager {

	

	public static void exportData(Driver driver, String url, String table, String filePath,String dateformat) throws Exception {
		BufferedWriter writer = null;
		ResultSetMetaData metaData = null;

		try {

			ConnectionManager.setupConnection( driver,url );

			GenericSQLReader reader = new GenericSQLReader(ConnectionManager.getConnection());

			List data = reader.getData("SELECT * FROM " + table);

			if (data != null && data.size() > 0) {
				metaData = reader.getTableMetaData(table);
				int columnCount = metaData.getColumnCount();

				writer = new BufferedWriter(new FileWriter(filePath));

				for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
					Map row = (Map) data.get(rowIndex);

					for (int colIndex = 0; colIndex < columnCount; colIndex++) {
						String colName = metaData.getColumnName(colIndex + 1);
						String colType = metaData.getColumnClassName(colIndex + 1);
						
						Object value = row.get(colName);

						if ( colType.equals("oracle.jdbc.OracleClob") ) {
							if(value != null){
								value = handleClob((CLOB) value, rowIndex, colIndex);
							}
						} 
						else if (colType.equals("oracle.sql.TIMESTAMP")) {
							if(value != null){
								oracle.sql.TIMESTAMP ts = new TIMESTAMP(value.toString());
								value = handleTimeStamp(ts.timestampValue(),dateformat);
							}
						}

						writer.write((value == null ? "NULL" : StringUtils.encodeString(value.toString()) ));
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
			ConnectionManager.cleanupConnection();
		}
	}

	private static String handleClob(CLOB value, int rowIndex, int colIndex) throws Exception {
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

	private static String handleTimeStamp(Timestamp date, String dateformat) {
		String formattedDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateformat);
		formattedDate = dateFormat.format(new java.util.Date(date.getTime()));
		return formattedDate;
	}

}
