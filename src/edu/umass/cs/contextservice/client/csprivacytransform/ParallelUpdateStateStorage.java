package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.List;

/**
 * Stores the state while parallel update sending.
 * In privacy case where one update results in updates to 
 * multiple anonymized IDs, this class is used to update all
 * anonymized IDs in parallel.
 * @author adipc
 */
public class ParallelUpdateStateStorage 
{
	private final List<CSUpdateTransformedMessage> updateMesgList;
	private final List<Long> finishTimeList;
	
	private long numCompletion 	= 0;
	
	private Object lock 		= new Object();
	
	public ParallelUpdateStateStorage(List<CSUpdateTransformedMessage> updateMesgList,
			List<Long> finishTimeList)
	{
		this.updateMesgList = updateMesgList;
		this.finishTimeList = finishTimeList;
	}
	
	public void incrementNumCompleted(long timeTaken)
	{
		synchronized(lock)
		{
			finishTimeList.add(timeTaken);
			numCompletion++;
			if(numCompletion == updateMesgList.size())
			{
				lock.notify();
			}
		}
	}
	
	public void waitForCompletion()
	{
		synchronized(lock)
		{
			while( numCompletion != updateMesgList.size() )
			{
				try 
				{
					lock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
}