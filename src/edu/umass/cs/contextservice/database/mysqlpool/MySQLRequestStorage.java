package edu.umass.cs.contextservice.database.mysqlpool;

import java.util.List;

import edu.umass.cs.contextservice.database.mysqlpool.callbacks.DatabaseCallBack;

public class MySQLRequestStorage
{
	// sometimes one or two queries are executed in one database call,
	// like check and insert, where first query is select and second is insert if needed.
	private final List<String> queriesToExecute;
	private final DatabaseRequestTypes requestType;
	private final DatabaseCallBack databaseCallBack;
	
	public MySQLRequestStorage( List<String> queriesToExecute, 
			DatabaseRequestTypes requestType, DatabaseCallBack databaseCallBack )
	{
		this.queriesToExecute = queriesToExecute;
		this.requestType = requestType;
		this.databaseCallBack = databaseCallBack;
	}
	
	public List<String> getQueryToExecute()
	{
		return this.queriesToExecute;
	}
	
	public DatabaseRequestTypes getRequestType()
	{
		return this.requestType;
	}
	
	public DatabaseCallBack getDatabaseCallBack()
	{
		return this.databaseCallBack;
	}
}