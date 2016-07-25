package edu.umass.cs.contextservice.database.mysqlpool;

import edu.umass.cs.contextservice.database.mysqlpool.callbacks.DatabaseCallBack;

public class MySQLRequestStorage 
{
	private final String queryToExecute;
	private final DatabaseRequestTypes requestType;
	private final DatabaseCallBack databaseCallBack;
	
	public MySQLRequestStorage(String queryToExecute, 
			DatabaseRequestTypes requestType, DatabaseCallBack databaseCallBack)
	{
		this.queryToExecute = queryToExecute;
		this.requestType = requestType;
		this.databaseCallBack = databaseCallBack;
	}
	
	public String getQueryToExecute()
	{
		return this.queryToExecute;
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