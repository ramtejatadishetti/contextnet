package edu.umass.cs.contextservice.client.callback.implementations;

import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.contextservice.client.callback.interfaces.CallBackInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.SearchReplyInterface;
import edu.umass.cs.contextservice.client.callback.interfaces.UpdateReplyInterface;

public class SampleCallBack implements CallBackInterface
{
	// key is the privacy req requesID,  value is the 
	// anonymized ID update tracker.
	// key is req ID, value is the start time
	private ConcurrentHashMap<Long, Long> requestIDMap;
	
	public SampleCallBack()
	{
		requestIDMap = new ConcurrentHashMap<Long, Long>();
	}
	
	@Override
	public void searchCompletion(SearchReplyInterface searchRep) 
	{	
		Long startTime = requestIDMap.get(searchRep.getCallerReqId());
		assert(startTime != null);

		System.out.println("Search completion time reqID "+searchRep.getCallerReqId()
						+ " time taken "+(System.currentTimeMillis()-startTime)
						+" replySize "+searchRep.getReplySize());
		
		requestIDMap.remove(searchRep.getCallerReqId());
	}
	
	@Override
	public void updateCompletion(UpdateReplyInterface updateRep) 
	{
		Long startTime = requestIDMap.get(updateRep.getCallerReqId());
		assert(startTime != null);

		System.out.println("Update completion time reqID "+updateRep.getCallerReqId()
						+ " time taken "+(System.currentTimeMillis()-startTime));
		
		requestIDMap.remove(updateRep.getCallerReqId());
	}
	
	/**
	 * @param contextServiceReqID
	 */
	public void addUpdateReply( long reqID )
	{
		requestIDMap.put(reqID, System.currentTimeMillis());
	}
}