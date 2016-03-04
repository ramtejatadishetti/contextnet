package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class UpdateTriggerMessage<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// On update, if an old value of an attribute was sent
	// old value sent to different node than new value
	public static final int OLD_VALUE				= 1;
	// On update, if a new value of an attribute was sent
	// new value sent to different node than old value
	public static final int NEW_VALUE				= 2;
	
	// both the old and new are on the same node
	public static final int BOTH					= 3;
	
	private enum Keys {REQUESTID, SUBSPACENUM, REPLICA_NUM, OLD_UPDATE_VALUE, OLD_NEW_VAL, 
		NEW_UPDATE_VALUE, NUM_REPLIES, ATTR_NAME};
	
	//update requestID of the update mesg
	private final long requestID;
	
	// subspace num
	private final int subspaceNum;
	
	private final JSONObject oldUpdateValPair;
	
	private final JSONObject newUpdateValPair;
	
	// flag indicates whether it is an
	// old value or a new value
	private final int oldNewVal;
	
	// number of trigger replies that will come back.
	// it can be either 1 or 2 depending on if old and new value fall 
	// on same node or not.
	private final int numReplies;
	
	private final int replicaNum;
	
	private final String attrName;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public UpdateTriggerMessage( NodeIDType initiator, long requestId, int subspaceNum, int replicaNum,
			JSONObject oldUpdateValPair, JSONObject newUpdateValPair, int oldNewVal, int numReplies, String attrName )
	{
		super(initiator, ContextServicePacket.PacketType.UPDATE_TRIGGER_MESSAGE);
		this.requestID = requestId;
		this.subspaceNum = subspaceNum;
		this.replicaNum = replicaNum;
		this.oldUpdateValPair = oldUpdateValPair;
		this.newUpdateValPair = newUpdateValPair;
		this.oldNewVal = oldNewVal;
		this.numReplies = numReplies;
		this.attrName = attrName;
	}
	
	public UpdateTriggerMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		this.replicaNum = json.getInt(Keys.REPLICA_NUM.toString());
		this.oldUpdateValPair = json.getJSONObject(Keys.OLD_UPDATE_VALUE.toString());
		this.newUpdateValPair = json.getJSONObject(Keys.NEW_UPDATE_VALUE.toString());
		this.oldNewVal = json.getInt(Keys.OLD_NEW_VAL.toString());
		this.numReplies = json.getInt(Keys.NUM_REPLIES.toString());
		this.attrName = json.getString(Keys.ATTR_NAME.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.REPLICA_NUM.toString(), this.replicaNum);
		json.put(Keys.OLD_UPDATE_VALUE.toString(), this.oldUpdateValPair);
		json.put(Keys.NEW_UPDATE_VALUE.toString(), this.newUpdateValPair);
		json.put(Keys.OLD_NEW_VAL.toString(), this.oldNewVal);
		json.put(Keys.NUM_REPLIES.toString(), this.numReplies);
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
	
	public JSONObject getOldUpdateValPair()
	{
		return this.oldUpdateValPair;
	}
	
	public JSONObject getNewUpdateValPair()
	{
		return this.newUpdateValPair;
	}
	
	public int getOldNewVal()
	{
		return this.oldNewVal;
	}
	
	public int getNumReplies()
	{
		return this.numReplies;
	}
	
	public String getAttrName()
	{
		return this.attrName;
	}
	
	public static void main(String[] args)
	{
	}
}