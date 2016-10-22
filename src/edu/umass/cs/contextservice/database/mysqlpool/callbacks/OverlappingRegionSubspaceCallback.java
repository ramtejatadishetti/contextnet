package edu.umass.cs.contextservice.database.mysqlpool.callbacks;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import edu.umass.cs.contextservice.database.mysqlpool.MySQLRequestStorage;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;

public class OverlappingRegionSubspaceCallback extends DatabaseCallBack
{
	private final HashMap<Integer, RegionInfoClass> answerObj;
	
	public OverlappingRegionSubspaceCallback
		( ConcurrentLinkedQueue<MySQLRequestStorage> codeRequestsQueue, 
				HashMap<Integer, RegionInfoClass> answerObj, 
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
	
	public HashMap<Integer, RegionInfoClass> 
												getAnswerObject()
	{
		return answerObj;
	}
}