package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class UpdateTriggerReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
//	// On update, if an old value of an attribute was sent
//	public static final int OLD_VALUE				= 1;
//	// On update, if a new value of an attribute was sent
//	public static final int NEW_VALUE				= 2;
	
	private enum Keys {REQUESTID, SUBSPACENUM, REPLICA_NUM, ADDED_GROUPS, REMOVED_GROUPS,
		NUM_REPLIES, OLD_NEW_BOTH, ATTR_NAME};
	
	//update requestID of the update mesg
	private final long requestID;
	
	// subspace num
	private final int subspaceNum;
	
	// replica num
	private final int replicaNum;
	//private final JSONObject updateValPair;
	
	// flag indicates whether it is an
	// old value or a new value
	//private final int oldNewVal;
	
	private final JSONArray toBeRemovedGroups;
	
	private final JSONArray toBeAddedGroups;
	
	private final int numReplies;
	
	private final int oldNewBoth;
	
	private final String attrName;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public UpdateTriggerReply( NodeIDType initiator, long requestId, int subspaceNum, int replicaNum, 
			JSONArray toBeRemovedGroups, JSONArray toBeAddedGroups, int numReplies, int oldNewBoth, String attrName )
	{
		super(initiator, ContextServicePacket.PacketType.UPDATE_TRIGGER_REPLY_MESSAGE);
		this.requestID = requestId;
		this.subspaceNum = subspaceNum;
		this.replicaNum = replicaNum;
		//this.updateValPair = updateValPair;
		//this.oldNewVal = oldNewVal;
		this.toBeRemovedGroups = toBeRemovedGroups;
		this.toBeAddedGroups = toBeAddedGroups;
		this.numReplies = numReplies;
		this.oldNewBoth = oldNewBoth;
		this.attrName = attrName;
	}
	
	public UpdateTriggerReply(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID   	   = json.getLong(Keys.REQUESTID.toString());
		this.subspaceNum 	   = json.getInt(Keys.SUBSPACENUM.toString());
		this.replicaNum  	   = json.getInt(Keys.REPLICA_NUM.toString());
		//this.updateValPair = json.getJSONObject(Keys.UPDATE_VALUE.toString());
		//this.oldNewVal = json.getInt(Keys.OLD_NEW_VAL.toString());
		this.toBeRemovedGroups = json.getJSONArray(Keys.REMOVED_GROUPS.toString());
		this.toBeAddedGroups   = json.getJSONArray(Keys.ADDED_GROUPS.toString());
		this.numReplies 	   = json.getInt(Keys.NUM_REPLIES.toString());
		this.oldNewBoth 	   = json.getInt(Keys.OLD_NEW_BOTH.toString());
		this.attrName 		   = json.getString(Keys.ATTR_NAME.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.REPLICA_NUM.toString(), this.replicaNum);
		json.put(Keys.REMOVED_GROUPS.toString(), this.toBeRemovedGroups);
		json.put(Keys.ADDED_GROUPS.toString(), this.toBeAddedGroups);
		json.put(Keys.NUM_REPLIES.toString(), this.numReplies);
		json.put(Keys.OLD_NEW_BOTH.toString(), this.oldNewBoth);
		json.put(Keys.ATTR_NAME.toString(), this.attrName);
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
	
	public int getReplicaNum()
	{
		return this.replicaNum;
	}
	
	public JSONArray getToBeRemovedGroups()
	{
		return this.toBeRemovedGroups;
	}
	
	public JSONArray getToBeAddedGroups()
	{
		return this.toBeAddedGroups;
	}
	
	public int getNumReplies()
	{
		return this.numReplies;
	}
	
	public int getOldNewBoth()
	{
		return this.oldNewBoth;
	}
	
	public String getAttrName()
	{
		return this.attrName;
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