package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class BulkGet<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {GetReqID, GUIDsToGet, Query};
	
	private final long getReqID;
	private final JSONArray guidToGet;
	// query is sent so that bulk get only returns GUIDs that satisfy query
	private final String query;
	
	public BulkGet(Integer initiator, long getReqID, JSONArray guidToGet, String query)
	{
		super(initiator, ContextServicePacket.PacketType.BULK_GET);
		this.getReqID = getReqID;
		this.guidToGet = guidToGet;
		this.query = query;
	}
	
	public BulkGet(JSONObject json) throws JSONException
	{
		super(json);
		this.getReqID = json.getLong(Keys.GetReqID.toString());
		this.guidToGet = json.getJSONArray(Keys.GUIDsToGet.toString());
		this.query = json.getString(Keys.Query.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GetReqID.toString(), this.getReqID);
		json.put(Keys.GUIDsToGet.toString(), guidToGet);
		json.put(Keys.Query.toString(), query);
		return json;
	}
	
	public JSONArray getGUIDsToGet()
	{
		return this.guidToGet;
	}
	
	public long getReqID()
	{
		return this.getReqID;
	}
	
	public String getQuery()
	{
		return this.query;
	}
	
	public static void main(String[] args)
	{
	}
}