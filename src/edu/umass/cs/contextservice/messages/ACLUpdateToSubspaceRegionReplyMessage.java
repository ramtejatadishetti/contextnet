package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ACLUpdateToSubspaceRegionReplyMessage 
							extends BasicContextServicePacket
{
	private enum Keys {VERSION_NUM, REQUEST_ID, 
									SUBSPACE_NUM, REPLICA_NUM};
	
	private final long versionNum;
	private final long requestID;
	private final int subspaceNum;
	private final int replicaNum;
	
	public ACLUpdateToSubspaceRegionReplyMessage( Integer initiator, long versionNum, 
			long requestID, int subspaceNum, int replicaNum )
	{
		super( initiator, 
				ContextServicePacket.PacketType.ACLUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE );
		this.versionNum  = versionNum;
		this.requestID 	 = requestID;
		this.subspaceNum = subspaceNum;
		this.replicaNum  = replicaNum;
	}
	
	public ACLUpdateToSubspaceRegionReplyMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACE_NUM.toString());
		this.replicaNum  = json.getInt(Keys.REPLICA_NUM.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.SUBSPACE_NUM.toString(), this.subspaceNum);
		json.put(Keys.REPLICA_NUM.toString(), this.replicaNum);
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
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}
	
	public int getReplicaNum()
	{
		return this.replicaNum;
	}
	
	public static void main( String[] args )
	{
	}
}