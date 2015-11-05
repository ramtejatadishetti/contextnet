package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;


public class QueryMesgToSubspaceRegionReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {REQUESTID, GROUP_GUID, RESULT_GUIDS, REPLY_SIZE};
	
	//private final NodeIDType sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	//private final String query;
	// GUID of group associated with this query
	private final String groupGUID;

	//private final int subspaceNum;
	private final JSONArray resultGUIDs;
	
	//just to indicate the reply size when 
	// actual replies are not sent
	private final int replySize;
	
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMesgToSubspaceRegionReply(NodeIDType initiator, long requestId, String groupGUID, JSONArray resultGUIDs)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION_REPLY);
		//this.predicate = predicate;
		//this.sourceNodeId = sourceID;
		this.requestID = requestId;
		this.groupGUID = groupGUID;
		
		if(ContextServiceConfig.sendFullReplies)
		{
			this.resultGUIDs = resultGUIDs;
			this.replySize = resultGUIDs.length();
		}
		else
		{
			this.resultGUIDs = new JSONArray();
			this.replySize = resultGUIDs.length();
		}
	}
	
	public QueryMesgToSubspaceRegionReply(JSONObject json) throws JSONException
	{
		super(json);
		//this.predicate = QueryComponent.getQueryComponent(json.getJSONObject(Keys.PREDICATE.toString()));
		//this.sourceNodeId = (NodeIDType)json.get(Keys.SOURCE_ID.toString());
		this.requestID   = json.getLong(Keys.REQUESTID.toString());
		this.groupGUID   = json.getString(Keys.GROUP_GUID.toString());
		this.resultGUIDs = json.getJSONArray(Keys.RESULT_GUIDS.toString());
		this.replySize   = json.getInt(Keys.REPLY_SIZE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		//json.put(Keys.PREDICATE.toString(), predicate.getJSONObject());
		//json.put(Keys.SOURCE_ID.toString(), sourceNodeId);
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.GROUP_GUID.toString(), groupGUID);
		json.put(Keys.RESULT_GUIDS.toString(), resultGUIDs);
		json.put(Keys.REPLY_SIZE.toString(), replySize);
		return json;
	}
	
	public long getRequestId()
	{
		return requestID;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public JSONArray getResultGUIDs()
	{
		return this.resultGUIDs;
	}
	
	public int returnReplySize()
	{
		return this.replySize;
	}
	
	public static void main(String[] args)
	{
	}
	
	/*public QueryComponent getQueryComponent()
	{
		return predicate;
	}
	public NodeIDType getSourceId()
	{
		return sourceNodeId;
	}
	public String getQuery()
	{
		return this.query;
	}
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}*/
}