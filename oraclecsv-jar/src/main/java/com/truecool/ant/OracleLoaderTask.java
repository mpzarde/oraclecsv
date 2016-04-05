package com.truecool.ant;

import oracle.jdbc.OracleDriver;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import com.truecool.managers.LoaderManager;

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


	public String getTable() {
		return _table;
	}

	public boolean isTruncate() {
		return _truncate;
	}

	public String getFilepath() {
		return _filepath;
	}


	public void execute() throws BuildException {

		try {
			LoaderManager.loadData(new OracleDriver(), getDbmsuri(), getTable(), getFilepath(), isTruncate(), getDateformat() );
		} catch (Exception e) {
			throw new BuildException(e);
		} 
	}

}
