package edu.umass.cs.contextservice.database.mysqlpool;

import java.util.concurrent.ConcurrentLinkedQueue;

import edu.umass.cs.contextservice.database.mysqlpool.callbacks.DatabaseCallBack;

public class MySQLRequestSubmitter implements RequestSubmitterInterface
{
	// one thing to note is that the javadoc says
	// size operation of this queue is not a constant time operation.
	// so should be avoided in use.
	private ConcurrentLinkedQueue<MySQLRequestStorage> 
					mysqlRequestsQueue;
	
	// mysql threads wait on this lock for a request. When
	// a request is added in the queue, then notify on this thread signals 
	// mysql threads to deque a request and execute.
	private final Object queueWaitLock = new Object();
	
	public MySQLRequestSubmitter()
	{
		mysqlRequestsQueue 
				= new ConcurrentLinkedQueue<MySQLRequestStorage>();
	}
	
	
	@Override
	public void submitRequest( String queryToExecute, 
			DatabaseRequestTypes requestType, DatabaseCallBack databaseCallBack )
	{
//		MySQLRequestStorage currMySQLRequest 
//						= new MySQLRequestStorage( queryToExecute, requestType, 
//								databaseCallBack );
//		
//		mysqlRequestsQueue.add(currMySQLRequest);
//		
//		synchronized( queueWaitLock )
//		{
//			queueWaitLock.notify();
//		}
	}
}