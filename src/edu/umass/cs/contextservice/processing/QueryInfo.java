package edu.umass.cs.contextservice.processing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;

import org.json.JSONArray;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;

/**
 * Class to store the query related information, 
 * like query, its source etc
 * @author ayadav
 */
public class QueryInfo<NodeIDType>
{
	// user query
	private final String query;
	private final NodeIDType sourceNodeId;
	private long requestId;
	private final String groupGUID;
	
	// req id set by the user
	private final long userReqID;
	private final String userIP;
	private final int userPort;
	
	// just for debugging and experimentation purpose
	private final long creationTime;
	
	//private final AbstractScheme<NodeIDType> scheme;
	// stores the parsed query components
	public Vector<QueryComponent> queryComponents;
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	public HashMap<Integer, LinkedList<String>> componentReplies;
	//public HashMap<Integer, JSONArray> componentReplies;
	
	private JSONArray hyperdexResultArray;
	
	// for synch
	private boolean requestCompl;
	
	// to store replies of each region of subspace
	public HashMap<Integer, JSONArray> regionalReplies;
	public HashMap<Integer, Integer> regionalRepliesSize;
	private final Object regionalRepliesLock = new Object();
	private int regionalRepliesCounter 		 = 0;
	
	
	public QueryInfo(String query, NodeIDType sourceNodeId, String grpGUID, 
			long userReqID, String userIP, int userPort, Vector<QueryComponent> queryComponents)
	{
		this.query = query;
		this.sourceNodeId = sourceNodeId;
		//this.requestId = requestID;
		this.groupGUID = grpGUID;
		//this.queryComponents = new Vector<QueryComponent>();
		this.queryComponents = queryComponents;
		this.componentReplies = new HashMap<Integer, LinkedList<String>>();
		//this.componentReplies = new HashMap<Integer, JSONArray>();
		//this.scheme = scheme;
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;
		
		this.creationTime = System.currentTimeMillis();
		regionalReplies = new HashMap<Integer, JSONArray>();
		regionalRepliesSize = new HashMap<Integer, Integer>();
		
		requestCompl = false;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public NodeIDType getSourceNodeId()
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
	
	/*public void setQueryComponents(Vector<QueryComponent> qc)
	{
		queryComponents.addAll(qc);
	}*/
	
	public void setQueryRequestID(long requestId)
	{
		this.requestId = requestId;
	}
	
	// just for experimentation purpose
	public long getCreationTime()
	{
		return this.creationTime;
	}
	
	public void setHyperdexResults(JSONArray hyperdexResults)
	{
		this.hyperdexResultArray = hyperdexResults;
	}
	
	public void setRequestCompl()
	{
		this.requestCompl = true;
	}
	
	public boolean getRequestCompl()
	{
		return this.requestCompl;
	}
	
	public JSONArray getHyperdexResults()
	{
		return this.hyperdexResultArray;
	}
	
	/**
	 * Initialize regional replies with number of regions 
	 * contacted for the search query
	 */
	public void initializeRegionalReplies(HashMap<Integer, JSONArray> regionalReplies)
	{
		this.regionalReplies = regionalReplies;
	}
	
	public boolean setRegionalReply(Integer senderID, 
			QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply)
	{
		synchronized(this.regionalRepliesLock)
		{
			if(ContextServiceConfig.sendFullReplies)
			{
				this.regionalReplies.put(senderID, queryMesgToSubspaceRegionReply.getResultGUIDs());
			}
			else
			{
				this.regionalRepliesSize.put(senderID, queryMesgToSubspaceRegionReply.returnReplySize());
			}
			
			regionalRepliesCounter++;
			
			// replies from all regions revd.
			if(regionalRepliesCounter == this.regionalReplies.size())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	public HashMap<Integer, JSONArray> getRepliesHashMap()
	{
		return this.regionalReplies;
	}
	
	public HashMap<Integer, Integer> getRepliesSizeHashMap()
	{
		return this.regionalRepliesSize;
	}
}