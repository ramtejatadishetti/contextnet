package edu.umass.cs.contextservice.client.callback.implementations;

import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class SampleUpdateReply implements UpdateReplyInterface
{
	private final int callerReqID;
	
	public SampleUpdateReply( int callerReqID )
	{
		this.callerReqID = callerReqID;
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
	}
}