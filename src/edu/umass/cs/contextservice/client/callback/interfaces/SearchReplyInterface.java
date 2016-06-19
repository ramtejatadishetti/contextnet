package edu.umass.cs.contextservice.client.callback.interfaces;

import org.json.JSONArray;

public interface SearchReplyInterface
{
	/**
	 * This function returns the search reply array 
	 * passed by the user.
	 * @return
	 */
	public long getCallerReqId();
	public void setSearchReplyArray(JSONArray replyArray);
	public void setReplySize(int replySize);
	public int getReplySize();
	public JSONArray getSearchReplyArray();
}