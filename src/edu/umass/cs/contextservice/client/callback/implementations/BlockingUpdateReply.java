package edu.umass.cs.contextservice.client.callback.implementations;

import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class BlockingUpdateReply implements UpdateReplyInterface
{
	private final long callerReqID;
	
	private final Object lock 	= new Object();
	private boolean completion 	= false;
	
	public BlockingUpdateReply( long callerReqID )
	{
		this.callerReqID = callerReqID;
	}
	
	public long getCallerReqId()
	{
		return callerReqID;
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