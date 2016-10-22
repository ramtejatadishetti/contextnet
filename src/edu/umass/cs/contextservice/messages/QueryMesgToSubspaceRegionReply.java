package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryMesgToSubspaceRegionReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys { REQUESTID, GROUP_GUID, RESULT_GUIDS, REPLY_SIZE, 
				PRIVACY_SCHEME, SUBSPACE_ID};
	
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	// private final String query;
	// GUID of group associated with this query
	private final String groupGUID;
	
	
	private final JSONArray resultGUIDs;
	
	//just to indicate the reply size when 
	// actual replies are not sent
	private final int replySize;
	
	
	private final int privacySchemeOrdinal;
	
	private final int subspaceId;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMesgToSubspaceRegionReply(NodeIDType initiator, long requestId, 
			String groupGUID, JSONArray resultGUIDs, int resultSize, int privacyScheme
			, int subspaceId)
	{
		super(initiator, 
				ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION_REPLY);
		
		this.requestID = requestId;
		this.groupGUID = groupGUID;
		
		this.resultGUIDs = resultGUIDs;
		this.replySize = resultSize;
		this.privacySchemeOrdinal = privacyScheme;
		this.subspaceId = subspaceId;
	}
	
	public QueryMesgToSubspaceRegionReply(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID   = json.getLong(Keys.REQUESTID.toString());
		this.groupGUID   = json.getString(Keys.GROUP_GUID.toString());
		this.resultGUIDs = json.getJSONArray(Keys.RESULT_GUIDS.toString());
		this.replySize   = json.getInt(Keys.REPLY_SIZE.toString());
		this.privacySchemeOrdinal = json.getInt(Keys.PRIVACY_SCHEME.toString());
		this.subspaceId = json.getInt(Keys.SUBSPACE_ID.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.GROUP_GUID.toString(), groupGUID);
		json.put(Keys.RESULT_GUIDS.toString(), resultGUIDs);
		json.put(Keys.REPLY_SIZE.toString(), replySize);
		json.put(Keys.PRIVACY_SCHEME.toString(), privacySchemeOrdinal);
		json.put(Keys.SUBSPACE_ID.toString(), subspaceId);
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
	
	public int getPrivacySchemeOrdinal()
	{
		return this.privacySchemeOrdinal;
	}
	
	public int getSubsapceId()
	{
		return this.subspaceId;
	}
	
	public static void main(String[] args)
	{
	}
}