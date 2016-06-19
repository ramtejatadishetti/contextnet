package edu.umass.cs.contextservice.client.storage;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;

public class SearchQueryStorage<NodeIDType>
{
	public long requestID;
	public QueryMsgFromUser<NodeIDType> queryMsgFromUser;
	public QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply;
	public SearchReplyInterface searchRep;
	public CallBackInterface callback;
}