package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


public class EchoMessage<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {ECHO_MESSAGE, SOURCEIP, SOURCEPORT};
	
	private final String echoMessage;
	private final String sourceIP;
	private final int sourcePort;
	
	public EchoMessage(NodeIDType initiator, String echoMessage, String sourceIP, int sourcePort)
	{
		super(initiator, ContextServicePacket.PacketType.ECHO_MESSAGE);
		this.echoMessage = echoMessage;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
	}
	
	public EchoMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.echoMessage = json.getString(Keys.ECHO_MESSAGE.toString());
		this.sourceIP = json.getString(Keys.SOURCEIP.toString());
		this.sourcePort = json.getInt(Keys.SOURCEPORT.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.ECHO_MESSAGE.toString(), echoMessage);
		json.put(Keys.SOURCEIP.toString(), this.sourceIP);
		json.put(Keys.SOURCEPORT.toString(), this.sourcePort);
		
		return json;
	}
	
	public String getEchoMessage()
	{
		return this.echoMessage;
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