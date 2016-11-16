package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateMsgToValuenodeReply<Integer> extends BasicContextServicePacket<Integer>
{
	private enum Keys {VERSION_NUM, NUM_REPLY, REQUEST_ID};
	
	private final long versionNum;
	private final int numReply;  // numReply to recv
	private final long requestID;
	
	public ValueUpdateMsgToValuenodeReply(Integer initiator, long versionNum, int numRep, long requestID)
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_TO_VALUENODE_REPLY);
		this.versionNum = versionNum;
		this.numReply = numRep;
		this.requestID = requestID;
	}
	
	public ValueUpdateMsgToValuenodeReply(JSONObject json) throws JSONException
	{
		//ValueUpdateFromGNS((Integer)0, json.getString(Keys.GUID.toString()), 
		//		json.getDouble(Keys.OLDVALUE.toString()), json.getDouble(Keys.NEWVALUE.toString()));
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.numReply = json.getInt(Keys.NUM_REPLY.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		//ContextServiceLogger.getLogger().fine("\n\n ValueUpdateFromGNS constructor");
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.NUM_REPLY.toString(), this.numReply);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public int getNumReply()
	{
		return this.numReply;
	}
	
	public long getRequestID()
	{
		return this.requestID;
	}
	
	public static void main(String[] args)
	{
	}
}