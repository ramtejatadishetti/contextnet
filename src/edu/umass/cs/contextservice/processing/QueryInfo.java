package edu.umass.cs.contextservice.processing;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Vector;

import edu.umass.cs.contextservice.schemes.AbstractScheme;

/**
 * Class to store the query related information, like query, its source etc.
 * 
 * @author ayadav
 */
public class QueryInfo<NodeIDType>
{
	// user query
	private final String query;
	private final NodeIDType sourceNodeId;
	private final long requestId;
	private final String groupGUID;
	
	// req id set by the user
	private final long userReqID;
	private final String userIP;
	private final int userPort;
	
	//private final AbstractScheme<NodeIDType> scheme;
	// stores the parsed query components
	public Vector<QueryComponent> queryComponents;
	// stores the replies recvd from the value nodes for the query
	// Hash map indexed by componentId, and Vector<String> stores 
	// the GUIDs
	public HashMap<Integer, LinkedList<String>> componentReplies;
	
	public QueryInfo(String query, NodeIDType sourceNodeId, long requestID, String grpGUID, 
			AbstractScheme<NodeIDType> scheme, long userReqID, String userIP, int userPort)
	{
		this.query = query;
		this.sourceNodeId = sourceNodeId;
		this.requestId = requestID;
		this.groupGUID = grpGUID;
		this.queryComponents = new Vector<QueryComponent>();
		this.componentReplies = new HashMap<Integer, LinkedList<String>>();
		//this.scheme = scheme;
		this.userReqID = userReqID;
		this.userIP = userIP;
		this.userPort = userPort;
	}
	
	public String getQuery()
	{
		return query;
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
	
	public void setQueryComponents(Vector<QueryComponent> qc)
	{
		queryComponents.addAll(qc);
	}
}