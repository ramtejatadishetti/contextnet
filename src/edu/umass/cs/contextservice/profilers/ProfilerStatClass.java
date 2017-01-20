package edu.umass.cs.contextservice.profilers;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;

public class ProfilerStatClass implements Runnable
{
	private long numNodesForSearchQuery 			= 0;
	private long numSearchReqs 						= 0;
	
	private long numNodesForUpdateReqs				= 0;
	private long numUpdateReqs 						= 0;
	
	
	private long incomingORate						= 0;
	private long incomingDRate						= 0;
	
	private long incomingSearchRate					= 0;
	
	private long numSearchInAttrIndex				= 0;
	private long numUpdateInAttrIndex				= 0;
	
	
	private final Object lock 						= new Object();
	
	@Override
	public void run()
	{
		while(true)
		{
			try
			{
				Thread.sleep(10000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			
			if( (numSearchReqs > 0) )
			{	
				System.out.println("numNodesForSearchQuery "
						+ (numNodesForSearchQuery/numSearchReqs)
						+ " numSearchReqs "+numSearchReqs );
			}
			
			if( (numUpdateReqs > 0) )
			{	
				System.out.println("numNodesForUpdateReqs "
						+ (numNodesForUpdateReqs/numUpdateReqs)
						+ " numUpdateReqs "+numUpdateReqs );
			}
			
			System.out.println("numSearchInAttrIndex "+numSearchInAttrIndex
					+" numUpdateInAttrIndex "+numUpdateInAttrIndex);
			
			
			double OutsearchDataThrouhgput  = 0.0;
			double OutsearchIndexThrouhgput = 0.0;
			
			double InsearchDataThrouhgput  = 0.0;
			double InsearchIndexThrouhgput = 0.0;
			
			double incomingSRate = 0.0;
			
			synchronized(lock)
			{	
				InsearchDataThrouhgput = (incomingDRate*1.0)/5.0;
				InsearchIndexThrouhgput = (incomingORate*1.0)/5.0;
				incomingDRate = 0;
				incomingORate = 0;
				
				incomingSRate = (incomingSearchRate*1.0)/5.0;
				incomingSearchRate = 0;
			}
			
			ContextServiceLogger.getLogger().fine("OutsearchDataThrouhgput "+OutsearchDataThrouhgput
					+ " OutsearchIndexThrouhgput "+OutsearchIndexThrouhgput
					+ " InsearchDataThrouhgput "+InsearchDataThrouhgput
					+ " InsearchIndexThrouhgput "+InsearchIndexThrouhgput
					+ " incomingSRate "+incomingSRate);
		}
	}
	
	public void incrementNumSearches(int currNumNodes)
	{
		synchronized(lock)
		{
			numNodesForSearchQuery = numNodesForSearchQuery + currNumNodes;
			numSearchReqs++;
		}
	}
	
	public void incrementNumSearchesAttrIndex()
	{
		synchronized(lock)
		{
			this.numSearchInAttrIndex++;
		}
	}
	
	public void incrementNumUpdatesAttrIndex()
	{
		synchronized(lock)
		{
			this.numUpdateInAttrIndex++;
		}
	}
	
	
	public void incrementNumUpdates(int currNumNodes)
	{
		synchronized(lock)
		{
			numNodesForUpdateReqs = numNodesForUpdateReqs + currNumNodes;
			numUpdateReqs++;
		}
	}
	
	
	public void incrementIncomingForOverlap()
	{
		synchronized( lock )
		{
			incomingORate++;
		}
	}
	
	
	public void incrementIncomingForData()
	{
		synchronized( lock )
		{
			incomingDRate++;
		}
	}
	
	public void incrementIncomingSearchRate()
	{
		synchronized( lock )
		{
			incomingSearchRate++;
		}
	}
}