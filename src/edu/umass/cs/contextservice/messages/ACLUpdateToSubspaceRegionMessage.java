package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ACLUpdateToSubspaceRegionMessage 
							extends BasicContextServicePacket
{
	// Value node that receives the message has to add entry, 
	// or remove entry or do remove and add both, in that order 
	public static final int ADD_ENTRY				= 1;
	public static final int REMOVE_ENTRY			= 2;
	public static final int UPDATE_ENTRY			= 3;
	
	private enum Keys {VERSION_NUM, GUID, UPDATE_ATTR_VAL_PAIRS, 
		OPER_TYPE, SUBSPACENUM, REQUEST_ID};
	
	private final long versionNum;
	//GUID of the update
	private final String GUID;
	
	// denotes attr value pairs that are updated in the update that generated this message.
	private final JSONObject updateAttrValuePairs;
	
	// old attr value pairs, contains all attributes, which are needed in ADD_ENTRY case,
	// as when an entry is added to a subspace all the attributes are added.
	
	private final int operType;
	private final int subspaceNum;
	
	private final long requestID;
	
	// is true if an entry is inserted first,
	// then UPDATE_ENTRY is treated as insert.
	// even when both old and new val fall on one node.
	
	public ACLUpdateToSubspaceRegionMessage( Integer initiator, long versionNum, String GUID, 
			JSONObject updateAttrValuePairs, int operType, int subspaceNum, long requestID )
	{
		super(initiator, ContextServicePacket.PacketType.ACLUPDATE_TO_SUBSPACE_REGION_MESSAGE);
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.updateAttrValuePairs = updateAttrValuePairs;
		this.operType = operType;
		this.subspaceNum = subspaceNum;
		this.requestID = requestID;
	}
		
	public ACLUpdateToSubspaceRegionMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUID.toString());
		this.updateAttrValuePairs = json.getJSONObject(Keys.UPDATE_ATTR_VAL_PAIRS.toString());
		this.operType = json.getInt(Keys.OPER_TYPE.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		this.requestID = json.getLong( Keys.REQUEST_ID.toString() );
	}
		
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUID.toString(), GUID);
		json.put(Keys.UPDATE_ATTR_VAL_PAIRS.toString(), this.updateAttrValuePairs);
		json.put(Keys.OPER_TYPE.toString(), this.operType);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		return json;
	}
	
	public String getGUID()
	{
		return this.GUID;
	}
	
	public JSONObject getUpdateAttrValuePairs()
	{
		return this.updateAttrValuePairs;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public int getOperType()
	{
		return this.operType;
	}
	
	public int getSubspaceNum()
	{
		return this.subspaceNum;
	}
	
	public long getRequestID()
	{
		return this.requestID;
	}
	
	public static void main( String[] args )
	{
	}
}