package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


public class EchoReplyMessage<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {ECHOREPLY_MESSAGE};
	
	private final String echoReplyMessage;
	
	public EchoReplyMessage(Integer initiator, String echoReplyMessage)
	{
		super(initiator, ContextServicePacket.PacketType.ECHOREPLY_MESSAGE);
		this.echoReplyMessage = echoReplyMessage;
	}
	
	public EchoReplyMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.echoReplyMessage = json.getString(Keys.ECHOREPLY_MESSAGE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.ECHOREPLY_MESSAGE.toString(), echoReplyMessage);
		return json;
	}
	
	public String getEchoMessage()
	{
		return this.echoReplyMessage;
	}
	
	public static void main(String[] args)
	{
	}
}