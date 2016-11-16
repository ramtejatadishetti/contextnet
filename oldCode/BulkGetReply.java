package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class BulkGetReply<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {RequestID, GUIDRecords};
	
	private final long requestID;
	private final JSONArray guidRecords;
	
	public BulkGetReply(Integer initiator, long requestID, JSONArray guidRecords)
	{
		super(initiator, ContextServicePacket.PacketType.BULK_GET_REPLY);
		this.requestID   = requestID;
		this.guidRecords = guidRecords;
	}
	
	public BulkGetReply(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.RequestID.toString());
		this.guidRecords = json.getJSONArray( Keys.GUIDRecords.toString() );
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.RequestID.toString(), this.requestID);
		json.put(Keys.GUIDRecords.toString(), guidRecords);
		return json;
	}
	
	public JSONArray getGUIDRecords()
	{
		return this.guidRecords;
	}
	
	public long getReqID()
	{
		return this.requestID;
	}
	
	public static void main(String[] args)
	{
	}
}