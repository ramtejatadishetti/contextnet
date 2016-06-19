package edu.umass.cs.contextservice.client.callback.implementations;

import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class PrivacyUpdateReply implements UpdateReplyInterface
{
	private long callerReqId;
	
	public PrivacyUpdateReply(long callerReqId)
	{
		this.callerReqId = callerReqId;
	}

	@Override
	public long getCallerReqId() 
	{
		return callerReqId;
	}
}