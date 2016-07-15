package edu.umass.cs.contextservice.profilers;

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
	
	private final Object lock 						= new Object();
	
	@Override
	public void run()
	{
		while(true)
		{
			try
			{
				Thread.sleep(5000);
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
			
			double searchDataThrouhgput  = 0.0;
			double searchIndexThrouhgput = 0.0;
			
			synchronized(lock)
			{
				searchDataThrouhgput = (searchDataDbOperation*1.0)/5.0;
				searchIndexThrouhgput = (searchIndexDbOperation*1.0)/5.0;
				
				searchDataDbOperation =0;
				searchIndexDbOperation = 0;
			}
			
			System.out.println("searchDataThrouhgput "+searchDataThrouhgput
					+" searchIndexThrouhgput "+searchIndexThrouhgput);
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
}