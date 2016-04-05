package com.truecool.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import com.truecool.managers.ExportManager;
import oracle.jdbc.OracleDriver;

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
		try {
			ExportManager.exportData( new OracleDriver(), getDbmsuri(), getTable(), getFilepath(), getDateformat());
		} catch (Exception e) {
			throw new BuildException(e);
		} 
	}


}
