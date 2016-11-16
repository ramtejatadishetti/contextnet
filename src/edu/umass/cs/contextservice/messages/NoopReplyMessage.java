package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class NoopReplyMessage extends BasicContextServicePacket
{
	// query is sent so that bulk get only returns GUIDs that satisfy query
	//private final String query;
	
	public NoopReplyMessage(Integer initiator)
	{
		super(initiator, ContextServicePacket.PacketType.NOOP_REPLY_MESSAGE);
	}
	
	public NoopReplyMessage(JSONObject json) throws JSONException
	{
		super(json);
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		return json;
	}
	
	public static void main(String[] args)
	{
	}
}