package edu.umass.cs.contextservice.profilers;

public class ProfilerStatClass implements Runnable
{
	private long numNodesForSearchQuery 			= 0;
	private long numSearchReqs 						= 0;
	
	private long numSubspaceRegionMesg 				= 0;
	private long numRepliesFromASubspaceRegion		= 0;
	
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
				System.out.println("numNodesForSearchQuery "+(numNodesForSearchQuery/numSearchReqs)
						+" numSearchReqs "+numSearchReqs 
						+" numRepliesFromASubspaceRegion "+(numRepliesFromASubspaceRegion/numSubspaceRegionMesg)
						+" numSubspaceRegionMesg "+numSubspaceRegionMesg );
			}
			
			//ContextServiceLogger.getLogger().fine("QueryFromUserRate "+diff1+" QueryFromUserDepart "+diff2+" QuerySubspaceRegion "+diff3+
			//		" QuerySubspaceRegionReply "+diff4+
			//		" DelayProfiler stats "+DelayProfiler.getStats());
			
			//ContextServiceLogger.getLogger().fine( "Pending query requests "+pendingQueryRequests.size() );
			//ContextServiceLogger.getLogger().fine("DelayProfiler stats "+DelayProfiler.getStats());
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
	
	public void incrementNumRepliesFromSubspaceRegion(int numReplies)
	{
		synchronized( lock )
		{
			numRepliesFromASubspaceRegion = numRepliesFromASubspaceRegion + numReplies;
			numSubspaceRegionMesg++;
		}
	}
}