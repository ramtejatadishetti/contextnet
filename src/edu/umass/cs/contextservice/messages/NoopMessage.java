package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


public class NoopMessage extends BasicContextServicePacket
{
	private enum Keys {SOURCEIP, SOURCEPORT, PAYLOAD};
	
	//private final long userReqId;
	//private final String guidToGet;
	private final String sourceIP;
	private final int sourcePort;
	private final String payloadString;
	// query is sent so that bulk get only returns GUIDs that satisfy query
	//private final String query;
	
	public NoopMessage(Integer initiator, String sourceIP, int sourcePort, 
			String payloadStr)
	{
		super(initiator, ContextServicePacket.PacketType.NOOP_MEESAGE);
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
		this.payloadString = payloadStr;
	}
	
	public NoopMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.sourceIP   = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
		this.payloadString = json.getString(Keys.PAYLOAD.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SOURCEIP.toString(), this.sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		json.put(Keys.PAYLOAD.toString(), this.payloadString);
		return json;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public String getPayloadString()
	{
		return this.payloadString;
	}
	
	public static void main(String[] args)
	{
	}
}