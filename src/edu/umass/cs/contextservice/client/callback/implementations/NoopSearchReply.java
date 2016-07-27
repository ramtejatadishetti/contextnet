package edu.umass.cs.contextservice.client.callback.implementations;

import org.json.JSONArray;

import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;

public class NoopSearchReply implements SearchReplyInterface
{
	private final int callerReqID;
	private JSONArray replyArray;
	private int replySize;
	
	public NoopSearchReply(int callerReqID)
	{
		this.callerReqID = callerReqID;
	}
	
	@Override
	public long getCallerReqId()
	{
		return callerReqID;
	}

	@Override
	public void setSearchReplyArray(JSONArray replyArray)
	{
		this.replyArray = replyArray;
	}
	
	@Override
	public void setReplySize(int replySize)
	{
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
}