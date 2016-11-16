package edu.umass.cs.contextservice.client.storage;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;

public class SearchQueryStorage
{
	public long requestID;
	public QueryMsgFromUser queryMsgFromUser;
	public QueryMsgFromUserReply queryMsgFromUserReply;
	public SearchReplyInterface searchRep;
	public CallBackInterface callback;
}