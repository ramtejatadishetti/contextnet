package edu.umass.cs.contextservice.database.mysqlpool.callbacks;

import java.util.concurrent.ConcurrentLinkedQueue;

import edu.umass.cs.contextservice.database.mysqlpool.MySQLRequestStorage;

public class CheckAndInsertSearchQueryInPrimaryTriggerSubspaceCallBack 
													extends DatabaseCallBack
{
	private final boolean recordFound;
	
	public CheckAndInsertSearchQueryInPrimaryTriggerSubspaceCallBack
		( ConcurrentLinkedQueue<MySQLRequestStorage> codeRequestsQueue, 
			MySQLRequestStorage currMySQLRequest )
	{
		super(codeRequestsQueue, currMySQLRequest);
//		assert(answerObj != null);
//		this.answerObj = answerObj;
		recordFound = false;
	}
	
	@Override
	public void requestCompletion()
	{
	}
}