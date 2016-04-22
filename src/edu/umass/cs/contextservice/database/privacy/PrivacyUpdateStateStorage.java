package edu.umass.cs.contextservice.database.privacy;

import java.util.Vector;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * This class stores the state of privacy update 
 * or deletions that happen for multiple attributes in parallel.
 * @author adipc
 */
public class PrivacyUpdateStateStorage<NodeIDType> 
{
	private final Object lock = new Object();
	private int numFinished;
	
	private Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdVect;
	
	public PrivacyUpdateStateStorage(Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdVect)
	{
		this.attrUpdVect = attrUpdVect;
		numFinished = 0;
	}
	
	
	public void indicateFinished()
	{
		synchronized( lock )
		{
			numFinished++;
			if( numFinished == attrUpdVect.size() )
			{
				lock.notify();
			}
		}
	}
	
	public void waitForFinish()
	{
		synchronized(lock)
		{
			while( numFinished != attrUpdVect.size() )
			{
				try 
				{
					lock.wait();
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
}