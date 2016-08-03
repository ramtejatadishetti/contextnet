package edu.umass.cs.contextservice.database.mysqlpool.callbacks;

import java.util.concurrent.ConcurrentLinkedQueue;

import edu.umass.cs.contextservice.database.mysqlpool.MySQLRequestStorage;

public abstract class DatabaseCallBack
{
	protected final ConcurrentLinkedQueue<MySQLRequestStorage> 
														codeRequestsQueue;
	
	protected final MySQLRequestStorage currMySQLRequest;
	
	public DatabaseCallBack( 
			ConcurrentLinkedQueue<MySQLRequestStorage> codeRequestsQueue, 
			MySQLRequestStorage currMySQLRequest )
	{
		this.codeRequestsQueue = codeRequestsQueue;
		this.currMySQLRequest = currMySQLRequest;
	}
	
	public abstract void requestCompletion();
}