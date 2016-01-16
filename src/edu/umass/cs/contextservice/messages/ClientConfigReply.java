package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClientConfigReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {NodeConfigArray, AttributeArray};
	
	private final JSONArray nodeConfigArray;
	private final JSONArray attrbuteArray;
	
	public ClientConfigReply(NodeIDType initiator, JSONArray nodeConfigArray,
			JSONArray attrbuteArray)
	{
		super(initiator, ContextServicePacket.PacketType.CONFIG_REPLY);
		this.nodeConfigArray = nodeConfigArray;
		this.attrbuteArray = attrbuteArray;
	}
	
	public ClientConfigReply(JSONObject json) throws JSONException
	{
		super(json);
		this.nodeConfigArray = json.getJSONArray(Keys.NodeConfigArray.toString());
		this.attrbuteArray = json.getJSONArray(Keys.AttributeArray.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.NodeConfigArray.toString(), this.nodeConfigArray);
		json.put(Keys.AttributeArray.toString(), this.attrbuteArray);
		return json;
	}
	
	public JSONArray getNodeConfigArray()
	{
		return this.nodeConfigArray;
	}
	
	public JSONArray getAttributeArray()
	{
		return this.attrbuteArray;
	}
	
	public static void main(String[] args)
	{
	}
}