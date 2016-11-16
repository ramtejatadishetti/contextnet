package edu.umass.cs.contextservice.updates;

import java.util.LinkedList;
import java.util.Queue;

/**
 * To store pending update requests for a GUID.
 * The operations of this class is synchronized by external lock.
 * 
 * @author adipc
 * @param <Integer>
 */
public class GUIDUpdateInfo
{
	// GUID associated with this object
	private final String GUID;
	private Queue<Long> pendingRequstsQueue;
	
	public GUIDUpdateInfo(String guid)
	{
		this.GUID = guid;
		pendingRequstsQueue = new LinkedList<Long>();
	}
	
	public String getGUID()
	{
		return this.GUID;
	}
	
	public void addUpdateReqNumToQueue(long requestId)
	{
		pendingRequstsQueue.add(requestId);
	}
	
	public Long removeFromQueue()
	{
		return pendingRequstsQueue.poll();
	}
	
	/**
	 * just returns the top of the queue without
	 * removing it.
	 * @return
	 */
	public Long getNextRequestID()
	{
		return pendingRequstsQueue.peek();
	}
}