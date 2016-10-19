package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateToSubspaceRegionMessage<NodeIDType> 
								extends BasicContextServicePacket<NodeIDType>
{
	// Value node that receives the message has to add entry, 
	// or remove entry or do remove and add both, in that order 
	public static final int ADD_ENTRY				= 1;
	public static final int REMOVE_ENTRY			= 2;
	public static final int UPDATE_ENTRY			= 3;
		
		
	private enum Keys { VERSION_NUM, GUID, JSON_TO_WRITE, 
		OPER_TYPE, SUBSPACENUM, REQUEST_ID, FIRST_TIME_INSERT, UPDATE_START_TIME, 
		OLD_VAL_JSON, NEW_UNSET_ATTRS, UPDATE_ATTR_VAL, PRIVACY_SCHEME };
	
	private final long versionNum;
	//GUID of the update
	private final String GUID;
	
	// jsonToWrite
	private final JSONObject jsonToWrite;
	
//	// old attr value pairs, contains all attributes, 
//	// which are needed in ADD_ENTRY case,
//	// as when an entry is added to a subspace all the attributes are added.
	private final JSONObject oldValJSON;
	
	//FIXME: need to optimize, as we are sending reducndant data here
	private final JSONObject updateAttrValJSON;
	
	private final JSONObject newUnsetAttrs;
	
	private final int operType;
	private final int subspaceNum;
	
	private final long requestID;
	
	// is true if an entry is inserted first,
	// then UPDATE_ENTRY is treated as insert.
	// even when both old and new val fall on one node.
	private final boolean firstTimeInsert;
	
	private final long updateStartTime;
	
	private final int privacySchemeOrdinal;
	
	
	public ValueUpdateToSubspaceRegionMessage( NodeIDType initiator, long versionNum, 
			String GUID, JSONObject jsonToWrite, int operType, int subspaceNum, 
			long requestID, boolean firstTimeInsert , long updateStartTime, 
			JSONObject oldValJSON, JSONObject newUnsetAttrs, JSONObject updateAttrJSON, 
			int privacySchemeOrdinal )
	{
		super( initiator, 
				ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE );
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.jsonToWrite = jsonToWrite;
		this.operType = operType;
		this.subspaceNum = subspaceNum;
		this.requestID = requestID;
		this.oldValJSON = oldValJSON;
		this.firstTimeInsert = firstTimeInsert;
		this.newUnsetAttrs = newUnsetAttrs;
		this.updateStartTime = updateStartTime;
		this.updateAttrValJSON = updateAttrJSON;
		this.privacySchemeOrdinal = privacySchemeOrdinal;
	}
	
	public ValueUpdateToSubspaceRegionMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUID.toString());
		this.jsonToWrite = json.getJSONObject(Keys.JSON_TO_WRITE.toString());
		this.operType = json.getInt(Keys.OPER_TYPE.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		this.requestID = json.getLong( Keys.REQUEST_ID.toString() );
		this.oldValJSON = json.getJSONObject(Keys.OLD_VAL_JSON.toString());
		this.firstTimeInsert = json.getBoolean(Keys.FIRST_TIME_INSERT.toString());
		this.newUnsetAttrs = json.getJSONObject(Keys.NEW_UNSET_ATTRS.toString());
		

		this.updateStartTime = json.getLong(Keys.UPDATE_START_TIME.toString());
		this.updateAttrValJSON = json.getJSONObject(Keys.UPDATE_ATTR_VAL.toString());
		this.privacySchemeOrdinal = json.getInt(Keys.PRIVACY_SCHEME.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUID.toString(), GUID);
		json.put(Keys.JSON_TO_WRITE.toString(), this.jsonToWrite);
		json.put(Keys.OPER_TYPE.toString(), this.operType);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		json.put(Keys.OLD_VAL_JSON.toString(), this.oldValJSON);
		json.put(Keys.FIRST_TIME_INSERT.toString(), this.firstTimeInsert);
		json.put(Keys.NEW_UNSET_ATTRS.toString(), this.newUnsetAttrs);

		json.put(Keys.UPDATE_START_TIME.toString(), this.updateStartTime);
		json.put(Keys.UPDATE_ATTR_VAL.toString(), this.updateAttrValJSON);
		json.put(Keys.PRIVACY_SCHEME.toString(), this.privacySchemeOrdinal);
		
		return json;
	}
	
	public String getGUID()
	{
		return this.GUID;
	}
	
	public JSONObject getJSONToWrite()
	{
		return this.jsonToWrite;
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
	
	public JSONObject getOldValJSON()
	{
		return this.oldValJSON;
	}
	
	public JSONObject getNewUnsetAttrs()
	{
		return this.newUnsetAttrs;
	}
	
	public boolean getFirstTimeInsert()
	{
		return this.firstTimeInsert;
	}
	
	public long getUpdateStartTime()
	{
		return this.updateStartTime;
	}
	
	public JSONObject getUpdateAttrValJSON()
	{
		return this.updateAttrValJSON;
	}
	
	public int getPrivacySchemeOrdinal()
	{
		return this.privacySchemeOrdinal;
	}
	
	public static void main(String[] args)
	{
	}
}