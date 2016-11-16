package edu.umass.cs.contextservice.profilers;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.updates.UpdateInfo;

public class ProfilerStatClass implements Runnable
{
	private long numNodesForSearchQuery 			= 0;
	private long numSearchReqs 						= 0;
	
	private long numSubspaceRegionMesg 				= 0;
	private long numRepliesFromASubspaceRegion		= 0;
	
	private long searchDataDbOperation				= 0;
	
	private long searchIndexDbOperation				= 0;
	
	private double overlapTimeSum					= 0;
	private double dataTimeSum						= 0;
	
	private long incomingORate						= 0;
	private long incomingDRate						= 0;
	
	private long incomingSearchRate					= 0;
	
	
	private final Object lock 						= new Object();
	
	private ConcurrentHashMap<Long, UpdateInfo> pendingUpdateRequests;
	
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
			
			if((numSearchReqs > 0) && (numSubspaceRegionMesg > 0) )
			{
				double avgOTime = (overlapTimeSum*1.0)/numSearchReqs;
				double avgDTime = (dataTimeSum*1.0)/numSubspaceRegionMesg;
				
				System.out.println("numNodesForSearchQuery "+(numNodesForSearchQuery/numSearchReqs)
						+" numSearchReqs "+numSearchReqs 
						+" numRepliesFromASubspaceRegion "+(numRepliesFromASubspaceRegion/numSubspaceRegionMesg)
						+" numSubspaceRegionMesg "+numSubspaceRegionMesg 
						+" avgOTime "+avgOTime 
						+" avgDTime "+avgDTime);
			}
			
			double OutsearchDataThrouhgput  = 0.0;
			double OutsearchIndexThrouhgput = 0.0;
			
			double InsearchDataThrouhgput  = 0.0;
			double InsearchIndexThrouhgput = 0.0;
			
			double incomingSRate = 0.0;
			
			synchronized(lock)
			{
				OutsearchDataThrouhgput = (searchDataDbOperation*1.0)/5.0;
				OutsearchIndexThrouhgput = (searchIndexDbOperation*1.0)/5.0;
				
				searchDataDbOperation =0;
				searchIndexDbOperation = 0;
				
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
			
			if(pendingUpdateRequests != null)
			{
				System.out.println("pendingUpdateRequests "+pendingUpdateRequests.size());
				if(pendingUpdateRequests.size() > 0)
				{
					Iterator<Long> reqIter = pendingUpdateRequests.keySet().iterator();
					
					UpdateInfo updInfo = pendingUpdateRequests.get(reqIter.next());
					
					System.out.println(" value update map string "+updInfo.toStringValueUpdateReplyMap());
				}
			}
			
			//ContextServiceLogger.getLogger().fine("QueryFromUserRate "+diff1+" QueryFromUserDepart "+diff2+" QuerySubspaceRegion "+diff3+
			//		" QuerySubspaceRegionReply "+diff4+
			//		" DelayProfiler stats "+DelayProfiler.getStats());
			
			//ContextServiceLogger.getLogger().fine( "Pending query requests "+pendingQueryRequests.size() );
			//ContextServiceLogger.getLogger().fine("DelayProfiler stats "+DelayProfiler.getStats());
		}
	}
	
	public void incrementNumSearches(int currNumNodes, long time)
	{
		synchronized(lock)
		{
			numNodesForSearchQuery = numNodesForSearchQuery + currNumNodes;
			numSearchReqs++;
			searchIndexDbOperation++;
			this.overlapTimeSum = overlapTimeSum+time;
		}
	}
	
	public void incrementNumRepliesFromSubspaceRegion(int numReplies, long time)
	{
		synchronized( lock )
		{
			numRepliesFromASubspaceRegion = numRepliesFromASubspaceRegion + numReplies;
			numSubspaceRegionMesg++;
			searchDataDbOperation++;
			this.dataTimeSum = dataTimeSum + time;
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
	
	
	public void monitorUpdateRequestsQueue(
			ConcurrentHashMap<Long, UpdateInfo> pendingUpdateRequests)
	{
		this.pendingUpdateRequests = pendingUpdateRequests;
	}
}