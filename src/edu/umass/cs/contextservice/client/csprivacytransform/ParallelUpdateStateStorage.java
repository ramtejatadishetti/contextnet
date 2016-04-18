package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.List;

/**
 * Stores the state while parallel update sending.
 * In privacy case where one update results in udpates to 
 * multiple anonymized IDs, this class os used to update all
 * anonymized IDs in parallel.
 * @author adipc
 */
public class ParallelUpdateStateStorage 
{
	private final List<CSUpdateTransformedMessage> updateMesgList;
	
	private long numCompletion 	= 0;
	
	private Object lock 		= new Object();
	
	public ParallelUpdateStateStorage(List<CSUpdateTransformedMessage> updateMesgList)
	{
		this.updateMesgList = updateMesgList;
	}
	
	public void incrementNumCompleted()
	{
		synchronized(lock)
		{
			numCompletion++;
			if(numCompletion == updateMesgList.size())
			{
				lock.notify();
			}
		}
	}
	
	public void waitForCompletion()
	{
		while( numCompletion != updateMesgList.size() )
		{
			synchronized(lock)
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