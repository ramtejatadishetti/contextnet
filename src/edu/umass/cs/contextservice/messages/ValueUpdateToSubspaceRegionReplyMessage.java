package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


public class ValueUpdateToSubspaceRegionReplyMessage<NodeIDType> 
										extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {VERSION_NUM, NUM_REPLY, REQUEST_ID, SUBSPACE_NUM, REPLICA_NUM};
	
	private final long versionNum;
	private final int numReply;  // numReply to recv
	private final long requestID;
	private final int subspaceNum;
	private final int replicaNum;
	
	public ValueUpdateToSubspaceRegionReplyMessage( NodeIDType initiator, long versionNum, 
			int numRep, long requestID, int subspaceNum, int replicaNum )
	{
		super(initiator, ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE);
		this.versionNum  = versionNum;
		this.numReply    = numRep;
		this.requestID 	 = requestID;
		this.subspaceNum = subspaceNum;
		this.replicaNum  = replicaNum;
	}
	
	public ValueUpdateToSubspaceRegionReplyMessage(JSONObject json) throws JSONException
	{
		//ValueUpdateFromGNS((NodeIDType)0, json.getString(Keys.GUID.toString()), 
		//		json.getDouble(Keys.OLDVALUE.toString()), json.getDouble(Keys.NEWVALUE.toString()));
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.numReply = json.getInt(Keys.NUM_REPLY.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACE_NUM.toString());
		this.replicaNum  = json.getInt(Keys.REPLICA_NUM.toString());
		//ContextServiceLogger.getLogger().fine("\n\n ValueUpdateFromGNS constructor");
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.NUM_REPLY.toString(), this.numReply);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.SUBSPACE_NUM.toString(), this.subspaceNum);
		json.put(Keys.REPLICA_NUM.toString(), this.replicaNum);
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
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}
	
	public int getReplicaNum()
	{
		return this.replicaNum;
	}
	
	public static void main(String[] args)
	{
	}
}