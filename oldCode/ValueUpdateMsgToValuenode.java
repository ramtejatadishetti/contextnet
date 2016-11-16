package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class ValueUpdateMsgToValuenode<Integer> extends BasicContextServicePacket<Integer>
{
	// Value node that receives the message has to add entry, 
	// or remove entry or do remove and add both, in that order 
	public static final int ADD_ENTRY				= 1;
	public static final int REMOVE_ENTRY			= 2;
	public static final int REMOVE_ADD_BOTH			= 3;
	// update entry is used in Mercury, where update to an attribute 
	// goes to all other attribute and other attributes just update 
	// their copy.
	public static final int UPDATE_ENTRY			= 4;
	
	
	private enum Keys {VERSION_NUM, GUIDs, ATTR_NAME, OLD_VAL, NEW_VAL, 
		OPER_TYPE, REQUEST_ID};
	
	private final long versionNum;
	//GUID of the update
	private final String GUID;
	private final String attributeName;
	// old value of attribute, in the beginning
	// the old value and the new value is same.
	private final double oldValue;
	private final double newValue;
	
	private final int operType;
	
	//private final JSONObject allAttrs;
	//private final Integer sourceID;
	
	private final long requestID;
	
	public ValueUpdateMsgToValuenode(Integer initiator, long versionNum, String GUID, String attrName, 
			double oldValue, double newValue, int operType, long requestID)
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_TO_VALUENODE);
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.attributeName = attrName;
		this.oldValue = oldValue;
		this.newValue = newValue;
		this.operType = operType;
		//this.allAttrs = allAttrs;
		//this.sourceID = sourceID;
		this.requestID = requestID;
	}
	
	public ValueUpdateMsgToValuenode(JSONObject json) throws JSONException
	{
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUIDs.toString());
		this.attributeName = json.getString(Keys.ATTR_NAME.toString());
		this.oldValue = json.getDouble(Keys.OLD_VAL.toString());
		this.newValue = json.getDouble(Keys.NEW_VAL.toString());
		this.operType = json.getInt(Keys.OPER_TYPE.toString());
		//this.allAttrs = json.getJSONObject(Keys.ALL_ATTRS.toString());
		//this.sourceID = (Integer)json.get(Keys.SOURCE_ID.toString());
		this.requestID = json.getLong(Keys.REQUEST_ID.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUIDs.toString(), GUID);
		json.put(Keys.ATTR_NAME.toString(), attributeName);
		json.put(Keys.OLD_VAL.toString(), oldValue);
		json.put(Keys.NEW_VAL.toString(), newValue);
		json.put(Keys.OPER_TYPE.toString(), this.operType);
		//json.put(Keys.ALL_ATTRS.toString(), this.allAttrs);
		//json.put(Keys.SOURCE_ID.toString(), this.sourceID);
		json.put(Keys.REQUEST_ID.toString(), this.requestID);
		return json;
	}
	
	public String getGUID()
	{
		return this.GUID;
	}
	
	public String getAttrName()
	{
		return this.attributeName;
	}
	
	public double getOldValue()
	{
		return this.oldValue;
	}
	
	public double getNewValue()
	{
		return this.newValue;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public int getOperType()
	{
		return this.operType;
	}
	
	/*public JSONObject getAllAttrs()
	{
		return this.allAttrs;
	}*/
	/*public Integer getSourceID()
	{
		return this.sourceID;
	}*/
	
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