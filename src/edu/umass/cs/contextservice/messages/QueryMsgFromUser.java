package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMsgFromUser<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, USER_REQ_NUM, EXPIRY_TIME, SOURCEIP, SOURCEPORT};
	
	private final String query;
	private final long userReqNum;
	// in is msecs from the time of query issue
	// like 60000 msec for 60 second query activation
	private final long expiryTime;
	
	private final String sourceIP;
	private final int sourcePort;
	
	public QueryMsgFromUser(NodeIDType initiator, String userQuery, long userReqNum, long expiryTime, 
			String sourceIP, int sourcePort)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_FROM_USER);
		this.query = userQuery;
		this.userReqNum = userReqNum;
		this.expiryTime = expiryTime;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
	}
	
	public QueryMsgFromUser(JSONObject json) throws JSONException
	{
		super(json);
		this.query = json.getString(Keys.QUERY.toString());
		this.userReqNum = json.getInt(Keys.USER_REQ_NUM.toString());
		this.expiryTime = json.getLong(Keys.EXPIRY_TIME.toString());
		this.sourceIP = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.USER_REQ_NUM.toString(), userReqNum);
		json.put(Keys.EXPIRY_TIME.toString(), expiryTime);
		json.put(Keys.SOURCEIP.toString(), sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
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
	
	public static void main(String[] args)
	{
	}
}