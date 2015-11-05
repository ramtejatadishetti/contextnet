package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.processing.QueryComponent;

/**
 * Query message that is sent to the metadata node.
 * After receiving the QueryMsgFromUser, QueryMsgToMetadataNode is sent
 * 
 * @author adipc
 * 
 */
public class QueryMsgToMetadataNode<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {PREDICATE, SOURCE_ID, REQUESTID, qUERY, GROUP_GUID};
	
	private final QueryComponent predicate;
	private final NodeIDType sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	private final String query;
	// GUID of group associated with this query
	private final String groupGUID;

	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMsgToMetadataNode(NodeIDType initiator, QueryComponent predicate, long requestId, 
			NodeIDType sourceID, String query, String groupGUID)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_TO_METADATANODE);
		this.predicate = predicate;
		this.sourceNodeId = sourceID;
		this.requestID = requestId;
		this.query = query;
		this.groupGUID = groupGUID;
	}

	@SuppressWarnings("unchecked")
	public QueryMsgToMetadataNode(JSONObject json) throws JSONException
	{
		super(json);
		this.predicate = QueryComponent.getQueryComponent(json.getJSONObject(Keys.PREDICATE.toString()));
		this.sourceNodeId = (NodeIDType)json.get(Keys.SOURCE_ID.toString());
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.query = json.getString(Keys.qUERY.toString());
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.PREDICATE.toString(), predicate.getJSONObject());
		json.put(Keys.SOURCE_ID.toString(), sourceNodeId);
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.qUERY.toString(), this.query);
		json.put(Keys.GROUP_GUID.toString(), this.groupGUID);
		return json;
	}

	public QueryComponent getQueryComponent()
	{
		return predicate;
	}
	
	public long getRequestId()
	{
		return requestID;
	}
	
	public NodeIDType getSourceId()
	{
		return sourceNodeId;
	}
	
	public String getQuery()
	{
		return this.query;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public static void main(String[] args)
	{
	}
}