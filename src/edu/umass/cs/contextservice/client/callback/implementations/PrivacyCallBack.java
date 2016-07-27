package edu.umass.cs.contextservice.client.callback.implementations;


import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class PrivacyCallBack implements CallBackInterface
{
	// key is the privacy req requesID,  value is the 
	// anonymized ID update tracker.
	
	public PrivacyCallBack()
	{
	}
	
	@Override
	public void searchCompletion(SearchReplyInterface searchRep) 
	{
		// Deons't need to be implemented in privacy.
		// as one user search only results in one search to context service.
		// So the user call back is used.
	}

	@Override
	public void updateCompletion(UpdateReplyInterface updateRep) 
	{
		// Needs to be implemented in privacy. As one user update 
		// results in multiple updates to context service, because multiple 
		// anonymized IDs are updated. So context service client uses its 
		// internal Privacy call back to keep tract of completion
		// on a privacy update. On completion the client uses 
		// user call back to signal privacy update completion 
		// to the user/application.
		((PrivacyUpdateReply)updateRep).incrementReplies();
	}
}