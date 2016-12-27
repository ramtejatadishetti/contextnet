package edu.umass.cs.contextservice.queryparsing;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.schemes.helperclasses.SearchReplyInfo;

/**
 * Class to store the query related information, 
 * like query, its source etc
 * @author ayadav
 */
public class QueryInfo
{	
	// user query
	private final String searchQuery;
	private  Integer sourceNodeId;
	private  long requestId;
	private  String groupGUID;
	
	// req id set by the user
	private  long userReqID;
	private  String userIP;
	private  int userPort;
	
	private long expiryTime;
	
	// for synch
	private boolean requestCompl;
	
	// only includes attributes that are specified in the query.
	private ValueSpaceInfo serachQueryValSpace;
	
	// for hyperspace privacy and no privacy case.
	// to store replies of each region of subspace
	//private HashMap<Integer, OverlappingInfoClass> regionalReplies;
	//private HashMap<Integer, Integer> regionalRepliesSize;
	
	// key is nodeid.
	private HashMap<Integer, SearchReplyInfo> searchReplyMap;
	
	private final Object addReplyLock = new Object();
	
	private int numRepliesRecvsSoFar = 0;
	
	public QueryInfo( String query, 
			Integer sourceNodeId, String grpGUID, long userReqID, 
			String userIP, int userPort, long expiryTime )
	{
		this.searchQuery = query;
		this.sourceNodeId = sourceNodeId;
		this.groupGUID = grpGUID;
		
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;
		this.expiryTime = expiryTime;
		
		searchReplyMap = new HashMap<Integer, SearchReplyInfo>();
		
		
		requestCompl = false;
		
		// query parsing
		serachQueryValSpace = QueryParser.parseQuery(query);
	}
	
	
	public ValueSpaceInfo getSearchQueryValSpace()
	{
		return serachQueryValSpace;
	}
	
	public String getQuery()
	{
		return searchQuery;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public Integer getSourceNodeId()
	{
		return sourceNodeId;
	}
	
	public long getRequestId()
	{
		return requestId;
	}
	
	public long getUserReqID()
	{
		return this.userReqID;
	}
	
	public String getUserIP()
	{
		return this.userIP;
	}
	
	public int getUserPort()
	{
		return this.userPort;
	}
	
	public void setQueryRequestID(long requestId)
	{
		this.requestId = requestId;
	}
	
	public void setRequestCompl()
	{
		this.requestCompl = true;
	}
	
	public boolean getRequestCompl()
	{
		return this.requestCompl;
	}
	
	public long getExpiryTime()
	{
		return this.expiryTime;
	}
	
	public HashMap<Integer, SearchReplyInfo> getSearchReplyMap()
	{
		return this.searchReplyMap;
	}
	
	/**
	 * Initialize regional replies with number of regions 
	 * contacted for the search query.
	 */
	public void initializeSearchQueryReplyInfo
						(List<Integer> searchReplyNodeList)
	{
		for(int i=0; i<searchReplyNodeList.size(); i++)
		{
			int nodeid = searchReplyNodeList.get(i);
			SearchReplyInfo searchReplyInfo = new SearchReplyInfo();
			searchReplyMap.put(nodeid, searchReplyInfo);
		}
	}
	
	public boolean addReplyFromANode(int senderID, 
			QueryMesgToSubspaceRegionReply queryMesgToSubspaceRegionReply)
	{
		synchronized(this.addReplyLock)
		{
			SearchReplyInfo subspaceSearchReply = searchReplyMap.get(senderID);
			
			if( ContextServiceConfig.sendFullRepliesWithinCS )
			{
				subspaceSearchReply.replyArray = queryMesgToSubspaceRegionReply.getResultGUIDs();
				subspaceSearchReply.numReplies = queryMesgToSubspaceRegionReply.returnReplySize();
			}
			else
			{
				subspaceSearchReply.numReplies = queryMesgToSubspaceRegionReply.returnReplySize();
			}
			
			numRepliesRecvsSoFar++;
			
			if( checkForRequestCompletion() )
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	/**
	 * This method requires synchronzied execution.
	 * Or atleast it is assumed right now.
	 * @return
	 */
	private boolean checkForRequestCompletion()
	{
		return numRepliesRecvsSoFar == searchReplyMap.size();
	}
}