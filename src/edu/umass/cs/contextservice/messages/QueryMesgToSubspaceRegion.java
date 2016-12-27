package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMesgToSubspaceRegion 
								extends BasicContextServicePacket
{
	private enum Keys { QUERY, REQUESTID, GROUP_GUID, USER_IP, USER_PORT
						, STORE_QUERY_FOR_TRIGGER, EXPIRY_TIME, PRIVACY_SCHEME };
	
	//private final Integer sourceNodeId;
	private final long requestID;
	
	// additional info for trigger to update groups on
	// value updates
	// whole query
	private final String query;
	// GUID of group associated with this query
	private final String groupGUID;
	
	private final String userIP;
	
	private final int userPort;
	
	private final boolean storeQueryForTrigger;
	private final long expiryTime;
	
	private final int privacySchemeOrdinal;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public QueryMesgToSubspaceRegion( Integer initiator, long requestId, String query, 
			String groupGUID, String userIP, int userPort, boolean storeQueryForTrigger, 
			long expiryTime,  int privacyScheme )
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION);
		
		this.requestID = requestId;
		this.query = query;
		this.groupGUID = groupGUID;
		
		this.userIP = userIP;
		this.userPort = userPort;
		this.storeQueryForTrigger = storeQueryForTrigger;
		this.expiryTime = expiryTime;
		this.privacySchemeOrdinal = privacyScheme;
	}
	
	public QueryMesgToSubspaceRegion(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.query = json.getString(Keys.QUERY.toString());
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.userIP = json.getString(Keys.USER_IP.toString());
		this.userPort = json.getInt(Keys.USER_PORT.toString());
		this.storeQueryForTrigger = json.getBoolean(Keys.STORE_QUERY_FOR_TRIGGER.toString());
		this.expiryTime = json.getLong(Keys.EXPIRY_TIME.toString());
		this.privacySchemeOrdinal = json.getInt(Keys.PRIVACY_SCHEME.toString());	
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.QUERY.toString(), this.query);
		json.put(Keys.GROUP_GUID.toString(), this.groupGUID);
		json.put(Keys.USER_IP.toString(), userIP);
		json.put(Keys.USER_PORT.toString(), userPort);
		json.put(Keys.STORE_QUERY_FOR_TRIGGER.toString(), this.storeQueryForTrigger);
		json.put(Keys.EXPIRY_TIME.toString(), this.expiryTime);
		json.put(Keys.PRIVACY_SCHEME.toString(), this.privacySchemeOrdinal);
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
	
	public String getUserIP()
	{
		return this.userIP;
	}
	
	public int getUserPort()
	{
		return this.userPort;
	}
	
	public boolean getStoreQueryForTrigger()
	{
		return this.storeQueryForTrigger;
	}
	
	public long getExpiryTime()
	{
		return this.expiryTime;
	}
	
	public int getPrivacyOrdinal()
	{
		return this.privacySchemeOrdinal;
	}
	
	public static void main(String[] args)
	{
	}
}