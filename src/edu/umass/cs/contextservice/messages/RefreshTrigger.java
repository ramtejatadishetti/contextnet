package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class RefreshTrigger<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, GROUP_GUID};
	
	private final String query;  // original query sent by the user.
	private final String groupGUID;
	
	public RefreshTrigger(NodeIDType initiator, String query, String groupGUID)
	{
		super(initiator, ContextServicePacket.PacketType.REFRESH_TRIGGER);
		
		this.groupGUID = groupGUID;
		this.query = query;
	}
	
	public RefreshTrigger(JSONObject json) throws JSONException
	{
		super(json);
		
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.query = json.getString(Keys.QUERY.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GROUP_GUID.toString(), groupGUID);
		json.put(Keys.QUERY.toString(), query);
		return json;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public static void main(String[] args)
	{
	}
}