package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMesgToSubspaceRegion<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, REQUESTID, GROUP_GUID, SUBSPACENUM};
	
	//private final NodeIDType sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	private final String query;
	// GUID of group associated with this query
	private final String groupGUID;

	private final int subspaceNum;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMesgToSubspaceRegion(NodeIDType initiator, long requestId, String query, String groupGUID, int subspaceNum)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION);
		//this.predicate = predicate;
		//this.sourceNodeId = sourceID;
		this.requestID = requestId;
		this.query = query;
		this.groupGUID = groupGUID;
		this.subspaceNum = subspaceNum;
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
		return json;
	}

	/*public QueryComponent getQueryComponent()
	{
		return predicate;
	}
	/*public NodeIDType getSourceId()
	{
		return sourceNodeId;
	}*/
	
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
	
	public static void main(String[] args)
	{
	}
}