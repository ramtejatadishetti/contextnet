package edu.umass.cs.contextservice.client.callback.implementations;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;

public class BlockingSearchReply implements SearchReplyInterface
{
	private final long callerReqID;
	private JSONArray replyArray;
	private int replySize;
	
	private final Object lock 	= new Object();
	private boolean completion 	= false;
	
	
	public BlockingSearchReply( long callerReqID )
	{
		this.callerReqID = callerReqID;
		completion = false;
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
	}

	@Override
	public void setSearchReplyArray(JSONArray replyArray)
	{
		assert(replyArray != null);
		this.replyArray = replyArray;
	}
	
	@Override
	public void setReplySize(int replySize)
	{
		assert(replySize >= 0);
		this.replySize = replySize;
	}
	
	@Override
	public int getReplySize()
	{
		return replySize;
	}
	
	@Override
	public JSONArray getSearchReplyArray()
	{
		return replyArray;
	}
	
	public void waitForCompletion()
	{
		synchronized( lock )
		{
			while( !completion )
			{
				try
				{
					lock.wait();
				}
				catch ( InterruptedException e )
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public void notifyCompletion()
	{
		synchronized( lock )
		{
			completion = true;
			lock.notify();
		}
	}
}