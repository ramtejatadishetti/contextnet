package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class GetMessage<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {USER_REQ_NUM, GUIDsToGet, SOURCEIP, SOURCEPORT};
	
	private final long userReqId;
	private final String guidToGet;
	private final String sourceIP;
	private final int sourcePort;
	// query is sent so that bulk get only returns GUIDs that satisfy query
	//private final String query;
	
	public GetMessage(NodeIDType initiator, long getReqID, String guidToGet, 
			String sourceIP, int sourcePort)
	{
		super(initiator, ContextServicePacket.PacketType.GET_MESSAGE);
		this.userReqId = getReqID;
		this.guidToGet = guidToGet;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
	}
	
	public GetMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.userReqId  = json.getLong(Keys.USER_REQ_NUM.toString());
		this.guidToGet  = json.getString(Keys.GUIDsToGet.toString());
		this.sourceIP   = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.USER_REQ_NUM.toString(), this.userReqId);
		json.put(Keys.GUIDsToGet.toString(), guidToGet);
		json.put(Keys.SOURCEIP.toString(), this.sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		return json;
	}
	
	public String getGUIDsToGet()
	{
		return this.guidToGet;
	}
	
	public long getUserReqID()
	{
		return this.userReqId;
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