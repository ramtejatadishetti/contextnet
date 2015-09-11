package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QuerierToRelayService<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {GUIDSet, MESSAGE};
	
	private final JSONArray guidSet;
	private final String message;
	
	public QuerierToRelayService(NodeIDType initiator, JSONArray guidSet, String message)
	{
		super(initiator, ContextServicePacket.PacketType.QUERIER_TO_RELAYSERVICE);
		this.guidSet = guidSet;
		this.message = message;
	}
	
	public QuerierToRelayService(JSONObject json) throws JSONException
	{
		super(json);
		this.guidSet = json.getJSONArray(Keys.GUIDSet.toString());
		this.message = json.getString(Keys.MESSAGE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GUIDSet.toString(), this.guidSet);
		json.put(Keys.MESSAGE.toString(), this.message);
		return json;
	}
	
	public JSONArray getGuidSet()
	{
		return this.guidSet;
	}
	
	public String getMessage()
	{
		return this.message;
	}
	
	public static void main(String[] args)
	{
	}
}