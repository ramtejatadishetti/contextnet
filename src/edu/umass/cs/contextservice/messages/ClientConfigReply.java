package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClientConfigReply extends BasicContextServicePacket
{
	private enum Keys {NodeConfigArray, AttributeArray, SubspaceConfigArray};
	
	private final JSONArray nodeConfigArray;
	private final JSONArray attrbuteArray;
	
	// each element is a JSONArray of attributes.
	private final JSONArray subspaceInfoArray;
	
	public ClientConfigReply( Integer initiator, JSONArray nodeConfigArray, 
			JSONArray attrbuteArray, JSONArray subspaceInfoArray )
	{
		super(initiator, ContextServicePacket.PacketType.CONFIG_REPLY);
		this.nodeConfigArray = nodeConfigArray;
		this.attrbuteArray = attrbuteArray;
		this.subspaceInfoArray = subspaceInfoArray;
	}
	
	public ClientConfigReply(JSONObject json) throws JSONException
	{
		super(json);
		this.nodeConfigArray = json.getJSONArray(Keys.NodeConfigArray.toString());
		this.attrbuteArray = json.getJSONArray(Keys.AttributeArray.toString());
		this.subspaceInfoArray = json.getJSONArray(Keys.SubspaceConfigArray.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.NodeConfigArray.toString(), this.nodeConfigArray);
		json.put(Keys.AttributeArray.toString(), this.attrbuteArray);
		json.put(Keys.SubspaceConfigArray.toString(), this.subspaceInfoArray);
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
	
	public JSONArray getSubspaceInfoArray()
	{
		return this.subspaceInfoArray;
	}
	
	public static void main( String[] args )
	{
	}
}