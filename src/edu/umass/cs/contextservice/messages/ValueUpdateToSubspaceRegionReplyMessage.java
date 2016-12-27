package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class ValueUpdateToSubspaceRegionReplyMessage 
										extends BasicContextServicePacket
{
	private enum Keys {VERSION_NUM, REQUEST_ID, TO_BE_REMOVED_GROUPS, TO_BE_ADDED_GROUPS};
	
	private final long versionNum;
	private final long requestID;
	private final JSONArray toBeRemovedGroups;
	private final JSONArray toBeAddedGroups;
	
	
	public ValueUpdateToSubspaceRegionReplyMessage( Integer initiator, long versionNum, 
			long requestID, JSONArray toBeRemovedGroups, JSONArray toBeAddedGroups )
	{
		super(initiator, 
			ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE);
		this.versionNum  = versionNum;
		this.requestID 	 = requestID;
		this.toBeRemovedGroups = toBeRemovedGroups;
		this.toBeAddedGroups = toBeAddedGroups;
	}
	
	public ValueUpdateToSubspaceRegionReplyMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.toBeRemovedGroups = json.getJSONArray(Keys.TO_BE_REMOVED_GROUPS.toString());
		this.toBeAddedGroups = json.getJSONArray(Keys.TO_BE_ADDED_GROUPS.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.TO_BE_REMOVED_GROUPS.toString(), this.toBeRemovedGroups);
		json.put(Keys.TO_BE_ADDED_GROUPS.toString(), this.toBeAddedGroups);
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public long getRequestID()
	{
		return this.requestID;
	}
	
	public JSONArray getToBeRemovedGroups()
	{
		return this.toBeRemovedGroups;
	}
	
	public JSONArray getToBeAddedGroups()
	{
		return this.toBeAddedGroups;
	}
	
	public static void main(String[] args)
	{
	}
}