package com.truecool.managers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Driver;

public class LoaderManager {



	public static void loadData(Driver driver, String url, String table, String filePath, boolean truncate,String dateformat) throws Exception {
		BufferedReader reader = null;

		try {

			ConnectionManager.setupConnection( driver,url );
			reader = new BufferedReader(new FileReader(filePath));

			if (truncate) {
				ConnectionManager.performTruncate(table);
			}

			String line = null;
			while ((line = reader.readLine()) != null) {
				ConnectionManager.handleLine(table, line,dateformat);
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	


	

}
