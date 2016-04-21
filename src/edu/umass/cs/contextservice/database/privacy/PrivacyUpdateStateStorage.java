package edu.umass.cs.contextservice.database.privacy;

import java.util.Vector;

/**
 * This class stores the state of privacy update 
 * or deletions that happen for multiple attributes in parallel.
 * @author adipc
 */
public class PrivacyUpdateStateStorage<NodeIDType> 
{
	private Object lock = new Object();
	private int numFinished	= 0;
	
	private Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdVect;
	
	public PrivacyUpdateStateStorage(Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdVect)
	{
		this.attrUpdVect = attrUpdVect;
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
		while( numFinished != attrUpdVect.size() )
		{
			synchronized(lock)
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