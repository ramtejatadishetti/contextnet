package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMesgToSubspaceRegion<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, REQUESTID, GROUP_GUID, SUBSPACENUM, USER_IP, USER_PORT};
	
	//private final NodeIDType sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	private final String query;
	// GUID of group associated with this query
	private final String groupGUID;

	private final int subspaceNum;
	
	private final String userIP;
	
	private final int userPort;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMesgToSubspaceRegion(NodeIDType initiator, long requestId, String query, 
			String groupGUID, int subspaceNum, String userIP, int userPort)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION);
		//this.predicate = predicate;
		//this.sourceNodeId = sourceID;
		this.requestID = requestId;
		this.query = query;
		this.groupGUID = groupGUID;
		this.subspaceNum = subspaceNum;
		
		this.userIP = userIP;
		this.userPort = userPort;
	}
	
	public QueryMesgToSubspaceRegion(JSONObject json) throws JSONException
	{
		super(json);
		//this.predicate = QueryComponent.getQueryComponent(json.getJSONObject(Keys.PREDICATE.toString()));
		//this.sourceNodeId = (NodeIDType)json.get(Keys.SOURCE_ID.toString());
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.query = json.getString(Keys.QUERY.toString());
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		this.userIP = json.getString(Keys.USER_IP.toString());
		this.userPort = json.getInt(Keys.USER_PORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		//json.put(Keys.PREDICATE.toString(), predicate.getJSONObject());
		//json.put(Keys.SOURCE_ID.toString(), sourceNodeId);
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.QUERY.toString(), this.query);
		json.put(Keys.GROUP_GUID.toString(), this.groupGUID);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.USER_IP.toString(), userIP);
		json.put(Keys.USER_PORT.toString(), userPort);
		return json;
	}
	
	public long getRequestId()
	{
		return requestID;
	}
	
	public String getQuery()
	{
		return this.query;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}
	
	public String getUserIP()
	{
		return this.userIP;
	}
	
	public int getUserPort()
	{
		return this.userPort;
	}
	
	public static void main(String[] args)
	{
	}
	
	/*public QueryComponent getQueryComponent()
	{
		return predicate;
	}
	/*public NodeIDType getSourceId()
	{
		return sourceNodeId;
	}*/
}