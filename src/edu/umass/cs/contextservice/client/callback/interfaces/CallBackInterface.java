package edu.umass.cs.contextservice.client.callback.interfaces;

/**
 * Call back interface for context service clients, mainly for 
 * non blocking calls.
 * @author adipc
 */
public interface CallBackInterface 
{
	/**
	 * Should be a nonblcking method. Context service client class this method
	 * to notify a search completion. Ideally the searchRep should be added in 
	 * a query and the reply processing thread in the application should be 
	 * notified here.
	 * @param searchRep
	 */
	public void searchCompletion(SearchReplyInterface searchRep);
	
	public void updateCompletion(UpdateReplyInterface updateRep);
}