package edu.umass.cs.contextservice.client.callback.implementations;


import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class NoopCallBack implements CallBackInterface
{	
	public NoopCallBack()
	{
	}
	
	@Override
	public void searchCompletion(SearchReplyInterface searchRep) 
	{
	}
	
	@Override
	public void updateCompletion(UpdateReplyInterface updateRep) 
	{
	}
}