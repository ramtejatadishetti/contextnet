package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class UpdateTriggerMessage<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// On update, if an old value of an attribute was sent 
	public static final int OLD_VALUE				= 1;
	// On update, if a new value of an attribute was sent
	public static final int NEW_VALUE				= 2;
	
	private enum Keys {REQUESTID, SUBSPACENUM, OLD_UPDATE_VALUE, OLD_NEW_VAL, 
		HASHCODE, NEW_UPDATE_VALUE};
	
	//update requestID of the update mesg
	private final long requestID;
	
	// subspace num
	private final int subspaceNum;
	
	private final JSONObject oldUpdateValPair;
	
	private final JSONObject newUpdateValPair;
	
	// flag indicates whether it is an
	// old value or a new value
	private final int oldNewVal;
	
	private final int hashCode;
	
	/*
	 * sourceID will be the ID of the node that 
	 * recvd query from the user.
	 */
	public UpdateTriggerMessage( NodeIDType initiator, long requestId, int subspaceNum, 
			JSONObject oldUpdateValPair, JSONObject newUpdateValPair, int oldNewVal, int hashCode )
	{
		super(initiator, ContextServicePacket.PacketType.UPDATE_TRIGGER_MESSAGE);
		this.requestID = requestId;
		this.subspaceNum = subspaceNum;
		this.oldUpdateValPair = oldUpdateValPair;
		this.newUpdateValPair = newUpdateValPair;
		this.oldNewVal = oldNewVal;
		this.hashCode = hashCode;
	}
	
	public UpdateTriggerMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.requestID = json.getLong(Keys.REQUESTID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		this.oldUpdateValPair = json.getJSONObject(Keys.OLD_UPDATE_VALUE.toString());
		this.newUpdateValPair = json.getJSONObject(Keys.NEW_UPDATE_VALUE.toString());
		this.oldNewVal = json.getInt(Keys.OLD_NEW_VAL.toString());
		this.hashCode = json.getInt(Keys.HASHCODE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.REQUESTID.toString(), requestID);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.OLD_UPDATE_VALUE.toString(), this.oldUpdateValPair);
		json.put(Keys.NEW_UPDATE_VALUE.toString(), this.newUpdateValPair);
		json.put(Keys.OLD_NEW_VAL.toString(), this.oldNewVal);
		json.put(Keys.HASHCODE.toString(), this.hashCode);
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
	
	public int getHashCode()
	{
		return this.hashCode;
	}
	
	public static void main(String[] args)
	{
	}
}