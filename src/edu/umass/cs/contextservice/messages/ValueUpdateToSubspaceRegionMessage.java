package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateToSubspaceRegionMessage<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// Value node that receives the message has to add entry, 
	// or remove entry or do remove and add both, in that order 
	public static final int ADD_ENTRY				= 1;
	public static final int REMOVE_ENTRY			= 2;
	public static final int UPDATE_ENTRY			= 3;
	
	private enum Keys {VERSION_NUM, GUID, ATTR_VAL_PAIRS, OPER_TYPE, SUBSPACENUM, REQUEST_ID};
	
	private final long versionNum;
	//GUID of the update
	private final String GUID;
	//private final String attributeName;
	// old value of attribute, in the beginning
	// the old value and the new value is same.
	//private final double oldValue;
	//private final double newValue;
	JSONObject attrValuePairs;
	
	private final int operType;
	private final int subspaceNum;
	
	private final long requestID;
	
	//private final JSONObject allAttrs;
	//private final NodeIDType sourceID;
	//private final long requestID;
	
	public ValueUpdateToSubspaceRegionMessage(NodeIDType initiator, long versionNum, String GUID, JSONObject attrValuePairs,
			int operType, int subspaceNum, long requestID)
	{
		super(initiator, ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE);
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.attrValuePairs = attrValuePairs;
		this.operType = operType;
		this.subspaceNum = subspaceNum;
		this.requestID = requestID;
		//this.allAttrs = allAttrs;
		//this.sourceID = sourceID;
		//this.requestID = requestID;
	}
	
	public ValueUpdateToSubspaceRegionMessage(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUID.toString());
		//this.attributeName = json.getString(Keys.ATTR_NAME.toString());
		//this.oldValue = json.getDouble(Keys.OLD_VAL.toString());
		//this.newValue = json.getDouble(Keys.NEW_VAL.toString());
		this.attrValuePairs = json.getJSONObject(Keys.ATTR_VAL_PAIRS.toString());
		this.operType = json.getInt(Keys.OPER_TYPE.toString());
		this.subspaceNum = json.getInt(Keys.SUBSPACENUM.toString());
		
		this.requestID = json.getLong( Keys.REQUEST_ID.toString() );
		//this.allAttrs = json.getJSONObject(Keys.ALL_ATTRS.toString());
		//this.sourceID = (NodeIDType)json.get(Keys.SOURCE_ID.toString());
		//this.requestID = json.getLong(Keys.REQUEST_ID.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUID.toString(), GUID);
		json.put(Keys.ATTR_VAL_PAIRS.toString(), this.attrValuePairs);
		json.put(Keys.OPER_TYPE.toString(), this.operType);
		json.put(Keys.SUBSPACENUM.toString(), this.subspaceNum);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		
		//json.put(Keys.ALL_ATTRS.toString(), this.allAttrs);
		//json.put(Keys.SOURCE_ID.toString(), this.sourceID);
		//json.put(Keys.REQUEST_ID.toString(), this.requestID);
		//json.put(Keys.OLD_VAL.toString(), oldValue);
		//json.put(Keys.NEW_VAL.toString(), newValue);
		return json;
	}
	
	public String getGUID()
	{
		return this.GUID;
	}
	
	public JSONObject getAttrValuePairs()
	{
		return this.attrValuePairs;
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
	
	public static void main(String[] args)
	{
		/*int[] group = {3, 45, 6, 19};
		MetadataMsgToValuenode<Integer> se = 
				new MetadataMsgToValuenode<Integer>(4, "name1", 2, Util.arrayToIntSet(group), Util.arrayToIntSet(group));
		try
		{
			ContextServiceLogger.getLogger().fine(se);
			MetadataMsgToValuenode<Integer> se2 = new MetadataMsgToValuenode<Integer>(se.toJSONObject());
			ContextServiceLogger.getLogger().fine(se2);
			assert(se.toString().length()==se2.toString().length());
			assert(se.toString().indexOf("}") == se2.toString().indexOf("}"));
			assert(se.toString().equals(se2.toString())) : se.toString() + "!=" + se2.toString();
		} catch(JSONException je)
		{
			je.printStackTrace();
		}*/
	}
}