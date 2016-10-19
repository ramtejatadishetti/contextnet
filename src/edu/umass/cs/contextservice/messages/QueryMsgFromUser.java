package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMsgFromUser<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, USER_REQ_NUM, EXPIRY_TIME, 
							SOURCEIP, SOURCEPORT, PRIVACY_SCHEME};
	
	private final String query;
	private final long userReqNum;
	// in is msecs from the time of query issue
	// like 60000 msec for 60 second query activation
	private final long expiryTime;
	
	private final String sourceIP;
	private final int sourcePort;
	
	// this is privacy scheme ordinal defined in PrivacySchemes enum in contextservice config.
	private final int privacyScheme;
	
	public QueryMsgFromUser(NodeIDType initiator, String userQuery, long userReqNum, 
			long expiryTime, String sourceIP, int sourcePort, int privacyScheme)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_FROM_USER);
		this.query = userQuery;
		this.userReqNum = userReqNum;
		this.expiryTime = expiryTime;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
		this.privacyScheme = privacyScheme;
	}
	
	public QueryMsgFromUser(JSONObject json) throws JSONException
	{
		super(json);
		this.query = json.getString(Keys.QUERY.toString());
		this.userReqNum = json.getInt(Keys.USER_REQ_NUM.toString());
		this.expiryTime = json.getLong(Keys.EXPIRY_TIME.toString());
		this.sourceIP = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
		
		this.privacyScheme = json.getInt(Keys.PRIVACY_SCHEME.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.USER_REQ_NUM.toString(), userReqNum);
		json.put(Keys.EXPIRY_TIME.toString(), expiryTime);
		json.put(Keys.SOURCEIP.toString(), sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		json.put(Keys.PRIVACY_SCHEME.toString(), this.privacyScheme);
		return json;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public long getUserReqNum()
	{
		return this.userReqNum;
	}
	
	public long getExpiryTime()
	{
		return this.expiryTime;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public int getPrivacySchemeOrdinal()
	{
		return this.privacyScheme;
	}
	
	public static void main(String[] args)
	{
	}
}