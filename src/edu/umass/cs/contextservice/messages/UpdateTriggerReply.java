package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class UpdateTriggerReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// On update, if an old value of an attribute was sent
	public static final int OLD_VALUE				= 1;
	// On update, if a new value of an attribute was sent
	public static final int NEW_VALUE				= 2;
	
	private enum Keys {REQUESTID, SUBSPACENUM, ADDED_GROUPS, REMOVED_GROUPS};
	
	//update requestID of the update mesg
	private final long requestID;
	
	// subspace num
	private final int subspaceNum;
	
	//private final JSONObject updateValPair;
	
	// flag indicates whether it is an
	// old value or a new value
	//private final int oldNewVal;
	
	private final JSONArray toBeRemovedGroups;
	
	private final JSONArray toBeAddedGroups;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public UpdateTriggerReply( NodeIDType initiator, long requestId, int subspaceNum, 
			JSONArray toBeRemovedGroups, JSONArray toBeAddedGroups)
	{
		super(initiator, ContextServicePacket.PacketType.UPDATE_TRIGGER_REPLY_MESSAGE);
		this.requestID = requestId;
		this.subspaceNum = subspaceNum;
		//this.updateValPair = updateValPair;
		//this.oldNewVal = oldNewVal;
		this.toBeRemovedGroups = toBeRemovedGroups;
		this.toBeAddedGroups = toBeAddedGroups;
	}
	
	public UpdateTriggerReply(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		//this.updateValPair = json.getJSONObject(Keys.UPDATE_VALUE.toString());
		//this.oldNewVal = json.getInt(Keys.OLD_NEW_VAL.toString());
		this.toBeRemovedGroups = json.getJSONArray(Keys.REMOVED_GROUPS.toString());
		this.toBeAddedGroups = json.getJSONArray(Keys.ADDED_GROUPS.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		//json.put(Keys.UPDATE_VALUE.toString(), this.updateValPair);
		//json.put(Keys.OLD_NEW_VAL.toString(), this.oldNewVal);
		json.put(Keys.REMOVED_GROUPS.toString(), this.toBeRemovedGroups);
		json.put(Keys.ADDED_GROUPS.toString(), this.toBeAddedGroups);
		return json;
	}
	
	public long getRequestId()
	{
		return requestID;
	}
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
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
	
	/*public JSONObject getUpdateValPair()
	{
		return this.updateValPair;
	}
	public int getOldNewVal()
	{
		return this.oldNewVal;
	}*/
}