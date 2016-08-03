package edu.umass.cs.contextservice.database.mysqlpool.callbacks;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.umass.cs.contextservice.database.mysqlpool.MySQLRequestStorage;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;

public class OverlappingRegionSubspaceCallback extends DatabaseCallBack
{
	private final HashMap<Integer, OverlappingInfoClass> answerObj;
	
	public OverlappingRegionSubspaceCallback
		( ConcurrentLinkedQueue<MySQLRequestStorage> codeRequestsQueue, 
				HashMap<Integer, OverlappingInfoClass> answerObj, 
				MySQLRequestStorage currMySQLRequest )
	{
		super(codeRequestsQueue, currMySQLRequest);
		assert(answerObj != null);
		this.answerObj = answerObj;
	}
	
	@Override
	public void requestCompletion()
	{
		this.codeRequestsQueue.add( this.currMySQLRequest );
		
		synchronized( this.codeRequestsQueue )
		{
			this.codeRequestsQueue.notify();
		}
	}
	
	public HashMap<Integer, OverlappingInfoClass> 
												getAnswerObject()
	{
		return answerObj;
	}
}