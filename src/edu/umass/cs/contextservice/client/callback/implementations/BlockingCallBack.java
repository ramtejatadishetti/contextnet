package edu.umass.cs.contextservice.client.callback.implementations;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class BlockingCallBack implements CallBackInterface
{
	public BlockingCallBack()
	{
	}
	
	@Override
	public void searchCompletion(SearchReplyInterface searchRep)
	{
		((BlockingSearchReply) searchRep).notifyCompletion();
	}
	
	
	@Override
	public void updateCompletion(UpdateReplyInterface updateRep)
	{
		((BlockingUpdateReply)updateRep).notifyCompletion();
	}
}